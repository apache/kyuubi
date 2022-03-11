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
import java.util.concurrent.{CountDownLatch, Executors}

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future, TimeoutException}
import scala.concurrent.duration._
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
import org.apache.kyuubi.engine.spark.events.{EngineEvent, EngineEventsStore, SparkEventLoggingService}
import org.apache.kyuubi.events.EventLogging
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.RetryPolicies
import org.apache.kyuubi.service.Serverable
import org.apache.kyuubi.util.SignalRegister

case class SparkSQLEngine(spark: SparkSession) extends Serverable("SparkSQLEngine") {

  override val backendService = new SparkSQLBackendService(spark)
  override val frontendServices = Seq(new SparkTBinaryFrontendService(this))

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
  }

  override protected def stopServer(): Unit = {
    countDownLatch.countDown()
  }
}

object SparkSQLEngine extends Logging {

  private var _sparkConf: SparkConf = _

  private var _kyuubiConf: KyuubiConf = _

  def kyuubiConf: KyuubiConf = _kyuubiConf

  var currentEngine: Option[SparkSQLEngine] = None

  private lazy val user = currentUser

  private val countDownLatch = new CountDownLatch(1)

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
      // start event logging ahead so that we can capture all statuses
      val eventLogging = new SparkEventLoggingService(spark.sparkContext)
      try {
        eventLogging.initialize(kyuubiConf)
        eventLogging.start()
      } catch {
        case NonFatal(e) =>
          // Don't block the main process if the `EventLoggingService` failed to start
          warn(s"Failed to initialize EventLoggingService: ${e.getMessage}", e)
      }

      try {
        engine.initialize(kyuubiConf)
        EventLogging.onEvent(EngineEvent(engine))
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
        EventLogging.onEvent(event)
      } catch {
        case t: Throwable =>
          throw new KyuubiException(s"Failed to start SparkSQLEngine: ${t.getMessage}", t)
      }
      // Stop engine before SparkContext stopped to avoid calling a stopped SparkContext
      addShutdownHook(() => engine.stop(), SPARK_CONTEXT_SHUTDOWN_PRIORITY + 2)
      addShutdownHook(() => eventLogging.stop(), SPARK_CONTEXT_SHUTDOWN_PRIORITY + 1)
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
      implicit val ec: ExecutionContextExecutorService =
        ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))
      try {
        val sparkFuture = Future {
          createSpark()
        }
        val timeout = initTimeout - totalInitTime
        spark = Await.result(sparkFuture, timeout.millisecond)
        try {
          startEngine(spark)
          // blocking main thread
          countDownLatch.await()
        } catch {
          case e: KyuubiException => currentEngine match {
              case Some(engine) =>
                engine.stop()
                val event = EngineEvent(engine).copy(diagnostic = e.getMessage)
                EventLogging.onEvent(event)
                error(event, e)
              case _ => error("Current SparkSQLEngine is not created.")
            }

        }
      } catch {
        case timeout: TimeoutException =>
          error(
            s"The engine initialization time exceeds" +
              s" `kyuubi.session.engine.initialize.timeout` ($initTimeout ms)" +
              s" and submitted at $submitTime.",
            timeout)
        case t: Throwable => error(s"Failed to instantiate SparkSession: ${t.getMessage}", t)
      } finally {
        ec.shutdown()
        if (spark != null) {
          spark.stop()
        }
      }
    }
  }
}
