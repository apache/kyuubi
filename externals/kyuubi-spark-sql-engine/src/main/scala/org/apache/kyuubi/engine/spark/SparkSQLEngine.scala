/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.engine.spark

import java.time.Instant
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import org.apache.spark.{ui, SparkConf}
import org.apache.spark.kyuubi.{SparkContextHelper, SparkSQLEngineEventListener, SparkSQLEngineListener}
import org.apache.spark.kyuubi.SparkUtilsHelper.getLocalDir
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.{KyuubiException, Logging, Utils}
import org.apache.kyuubi.Utils._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_ENGINE_SUBMIT_TIME_KEY
import org.apache.kyuubi.engine.spark.SparkSQLEngine.{countDownLatch, currentEngine}
import org.apache.kyuubi.engine.spark.events.{EngineEvent, EngineEventsStore}
import org.apache.kyuubi.engine.spark.events.handler.{SparkHistoryLoggingEventHandler, SparkJsonLoggingEventHandler}
import org.apache.kyuubi.events.{EventBus, EventLoggerType, KyuubiEvent}
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.{EngineServiceDiscovery, RetryPolicies}
import org.apache.kyuubi.service.Serverable
import org.apache.kyuubi.util.{SignalRegister, ThreadUtils}

case class SparkSQLEngine(spark: SparkSession) extends Serverable("SparkSQLEngine") {

  override val backendService = new SparkSQLBackendService(spark)
  override val frontendServices = Seq(new SparkTBinaryFrontendService(this))

  @volatile private var shutdown = false
  @volatile private var deregistered = false

  private val lifetimeTerminatingChecker =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("spark-engine-lifetime-checker")

  override def initialize(conf: KyuubiConf): Unit = {
    val listener = new SparkSQLEngineListener(this)
    spark.sparkContext.addSparkListener(listener)
    val kvStore = SparkContextHelper.getKvStore(spark.sparkContext)
    val engineEventListener = new SparkSQLEngineEventListener(kvStore, conf)
    spark.sparkContext.addSparkListener(engineEventListener)
    super.initialize(conf)
  }

  override def start(): Unit = {
    super.start()
    // Start engine self-terminating checker after all services are ready and it can be reached by
    // all servers in engine spaces.
    backendService.sessionManager.startTerminatingChecker(() => {
      assert(currentEngine.isDefined)
      currentEngine.get.stop()
    })

    startLifetimeTerminatingChecker(() => {
      assert(currentEngine.isDefined)
      currentEngine.get.stop()
    })
  }

  override def stop(): Unit = synchronized {
    super.stop()

    shutdown = true
    val shutdownTimeout = conf.get(ENGINE_EXEC_POOL_SHUTDOWN_TIMEOUT)
    ThreadUtils.shutdown(
      lifetimeTerminatingChecker,
      Duration(shutdownTimeout, TimeUnit.MILLISECONDS))
  }

  override protected def stopServer(): Unit = {
    countDownLatch.countDown()
  }

  private[kyuubi] def startLifetimeTerminatingChecker(stop: () => Unit): Unit = {
    val interval = conf.get(ENGINE_CHECK_INTERVAL)
    val maxLifetime = conf.get(ENGINE_SPARK_MAX_LIFETIME)
    if (maxLifetime > 0) {
      val checkTask = new Runnable {
        override def run(): Unit = {
          if (!shutdown && System.currentTimeMillis() - getStartTime > maxLifetime) {
            if (!deregistered) {
              info(s"Spark engine has been running for more than $maxLifetime ms," +
                s" deregistering from engine discovery space.")
              frontendServices.flatMap(_.discoveryService).map {
                case engineServiceDiscovery: EngineServiceDiscovery => engineServiceDiscovery.stop()
              }
              deregistered = true
            }

            if (backendService.sessionManager.getOpenSessionCount <= 0) {
              info(s"Spark engine has been running for more than $maxLifetime ms" +
                s" and no open session now, terminating")
              stop()
            }
          }
        }
      }
      lifetimeTerminatingChecker.scheduleWithFixedDelay(
        checkTask,
        interval,
        interval,
        TimeUnit.MILLISECONDS)
    }
  }
}

object SparkSQLEngine extends Logging {

  private var _sparkConf: SparkConf = _

  private var _kyuubiConf: KyuubiConf = _

  def kyuubiConf: KyuubiConf = _kyuubiConf

  var currentEngine: Option[SparkSQLEngine] = None

  private lazy val user = currentUser

  private val countDownLatch = new CountDownLatch(1)

  private val sparkSessionCreated = new AtomicBoolean(false)

  SignalRegister.registerLogger(logger)
  setupConf()

  def setupConf(): Unit = {
    _sparkConf = new SparkConf()
    _kyuubiConf = KyuubiConf()
    val rootDir = _sparkConf.getOption("spark.repl.classdir").getOrElse(getLocalDir(_sparkConf))
    val outputDir = Utils.createTempDir(root = rootDir, namePrefix = "repl")
    _sparkConf.setIfMissing("spark.sql.execution.topKSortFallbackThreshold", "10000")
    _sparkConf.setIfMissing("spark.sql.legacy.castComplexTypesToString.enabled", "true")
    _sparkConf.setIfMissing("spark.master", "local")
    _sparkConf.setIfMissing("spark.ui.port", "0")
    // register the repl's output dir with the file server.
    // see also `spark.repl.classdir`
    _sparkConf.set("spark.repl.class.outputDir", outputDir.toFile.getAbsolutePath)
    _sparkConf.setIfMissing(
      "spark.hadoop.mapreduce.input.fileinputformat.list-status.num-threads",
      "20")

    val appName = s"kyuubi_${user}_spark_${Instant.now}"
    _sparkConf.setIfMissing("spark.app.name", appName)
    val defaultCat = if (KyuubiSparkUtil.hiveClassesArePresent) "hive" else "in-memory"
    _sparkConf.setIfMissing("spark.sql.catalogImplementation", defaultCat)

    kyuubiConf.setIfMissing(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    kyuubiConf.setIfMissing(HA_ZK_CONN_RETRY_POLICY, RetryPolicies.N_TIME.toString)

    // Pass kyuubi config from spark with `spark.kyuubi`
    val sparkToKyuubiPrefix = "spark.kyuubi."
    _sparkConf.getAllWithPrefix(sparkToKyuubiPrefix).foreach { case (k, v) =>
      kyuubiConf.set(s"kyuubi.$k", v)
    }

    if (logger.isDebugEnabled) {
      kyuubiConf.getAll.foreach { case (k, v) =>
        debug(s"KyuubiConf: $k = $v")
      }
    }
  }

  def createSpark(): SparkSession = {
    val session = SparkSession.builder.config(_sparkConf).getOrCreate
    (kyuubiConf.get(ENGINE_INITIALIZE_SQL) ++ kyuubiConf.get(ENGINE_SESSION_INITIALIZE_SQL))
      .foreach { sqlStr =>
        session.sparkContext.setJobGroup(
          "engine_initializing_queries",
          sqlStr,
          interruptOnCancel = true)
        debug(s"Execute session initializing sql: $sqlStr")
        session.sql(sqlStr).isEmpty
        session.sparkContext.clearJobGroup()
      }
    session
  }

  def startEngine(spark: SparkSession): Unit = {
    currentEngine = Some(new SparkSQLEngine(spark))
    currentEngine.foreach { engine =>
      try {
        // start event logging ahead so that we can capture all statuses
        initLoggerEventHandler(kyuubiConf)
      } catch {
        case NonFatal(e) =>
          // Don't block the main process if the `LoggerEventHandler` failed to start
          warn(s"Failed to initialize LoggerEventHandler: ${e.getMessage}", e)
      }

      try {
        engine.initialize(kyuubiConf)
        EventBus.post(EngineEvent(engine))
      } catch {
        case t: Throwable =>
          throw new KyuubiException(s"Failed to initialize SparkSQLEngine: ${t.getMessage}", t)
      }
      try {
        engine.start()
        val kvStore = SparkContextHelper.getKvStore(spark.sparkContext)
        val store = new EngineEventsStore(kvStore)
        ui.EngineTab(
          Some(engine),
          SparkContextHelper.getSparkUI(spark.sparkContext),
          store,
          kyuubiConf)
        val event = EngineEvent(engine)
        info(event)
        EventBus.post(event)
      } catch {
        case t: Throwable =>
          throw new KyuubiException(s"Failed to start SparkSQLEngine: ${t.getMessage}", t)
      }
      // Stop engine before SparkContext stopped to avoid calling a stopped SparkContext
      addShutdownHook(() => engine.stop(), SPARK_CONTEXT_SHUTDOWN_PRIORITY + 2)
    }

    def initLoggerEventHandler(conf: KyuubiConf): Unit = {
      conf.get(ENGINE_EVENT_LOGGERS)
        .map(EventLoggerType.withName)
        .foreach {
          case EventLoggerType.SPARK =>
            EventBus.register[KyuubiEvent](new SparkHistoryLoggingEventHandler(spark.sparkContext))
          case EventLoggerType.JSON =>
            val handler = SparkJsonLoggingEventHandler(
              spark.sparkContext.applicationAttemptId
                .map(id => s"${spark.sparkContext.applicationId}_$id")
                .getOrElse(spark.sparkContext.applicationId),
              ENGINE_EVENT_JSON_LOG_PATH,
              spark.sparkContext.hadoopConfiguration,
              conf)

            // register JsonLogger as a event handler for default event bus
            EventBus.register[KyuubiEvent](handler)
          case _ =>
        }

    }

  }

  def main(args: Array[String]): Unit = {
    val startedTime = System.currentTimeMillis()
    val submitTime = kyuubiConf.getOption(KYUUBI_ENGINE_SUBMIT_TIME_KEY) match {
      case Some(t) => t.toLong
      case _ => startedTime
    }
    val initTimeout = kyuubiConf.get(ENGINE_INIT_TIMEOUT)
    val totalInitTime = startedTime - submitTime
    if (totalInitTime > initTimeout) {
      throw new KyuubiException(s"The total engine initialization time ($totalInitTime ms)" +
        s" exceeds `kyuubi.session.engine.initialize.timeout` ($initTimeout ms)," +
        s" and submitted at $submitTime.")
    } else {
      var spark: SparkSession = null
      try {
        startInitTimeoutChecker(submitTime, initTimeout)
        spark = createSpark()
        sparkSessionCreated.set(true)
        try {
          startEngine(spark)
          // blocking main thread
          countDownLatch.await()
        } catch {
          case e: KyuubiException => currentEngine match {
              case Some(engine) =>
                engine.stop()
                val event = EngineEvent(engine)
                  .copy(endTime = System.currentTimeMillis(), diagnostic = e.getMessage)
                EventBus.post(event)
                error(event, e)
              case _ => error("Current SparkSQLEngine is not created.")
            }

        }
      } catch {
        case i: InterruptedException if !sparkSessionCreated.get =>
          error(
            s"The Engine main thread was interrupted, possibly due to `createSpark` timeout." +
              s" The `kyuubi.session.engine.initialize.timeout` is ($initTimeout ms) " +
              s" and submitted at $submitTime.",
            i)
        case t: Throwable => error(s"Failed to instantiate SparkSession: ${t.getMessage}", t)
      } finally {
        if (spark != null) {
          spark.stop()
        }
      }
    }
  }

  private def startInitTimeoutChecker(startTime: Long, timeout: Long): Unit = {
    val mainThread = Thread.currentThread()
    new Thread(
      () => {
        while (System.currentTimeMillis() - startTime < timeout && !sparkSessionCreated.get()) {
          Thread.sleep(500)
        }
        if (!sparkSessionCreated.get()) {
          mainThread.interrupt()
        }
      },
      "CreateSparkTimeoutChecker").start()
  }
}
