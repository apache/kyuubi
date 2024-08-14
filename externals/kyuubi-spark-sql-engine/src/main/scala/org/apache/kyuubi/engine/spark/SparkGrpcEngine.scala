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

import java.util.concurrent.{CountDownLatch, ScheduledExecutorService, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration.Duration

import org.apache.spark.SparkConf
import org.apache.spark.kyuubi.SparkSQLEngineListener
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.{KyuubiException, Logging, Utils}
import org.apache.kyuubi.Utils._
import org.apache.kyuubi.config.{KyuubiConf, KyuubiReservedKeys}
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_ENGINE_URL
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.engine.spark.SparkGrpcEngine.{countDownLatch, currentEngine}
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.RetryPolicies
import org.apache.kyuubi.service.Serverable
import org.apache.kyuubi.util.{SignalRegister, ThreadUtils}
import org.apache.kyuubi.util.ThreadUtils.scheduleTolerableRunnableWithFixedDelay

class SparkGrpcEngine(spark: SparkSession) extends Serverable("SparkGrpcEngine") {

  override val backendService = new SparkGrpcBackendService(spark)
  override val frontendServices = Seq(new SparkGrpcFrontendService(this))

  private val shutdown = new AtomicBoolean(false)
  private val gracefulStopDeregistered = new AtomicBoolean(false)

  @volatile private var lifetimeTerminatingChecker: Option[ScheduledExecutorService] = None
  @volatile private var stopEngineExec: Option[ThreadPoolExecutor] = None

  override def initialize(conf: KyuubiConf): Unit = {
    val listener = new SparkSQLEngineListener(this)
    spark.sparkContext.addSparkListener(listener)
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

    val maxInitTimeout = conf.get(ENGINE_SPARK_MAX_INITIAL_WAIT)
    if (conf.get(ENGINE_SHARE_LEVEL) == ShareLevel.CONNECTION.toString &&
      maxInitTimeout > 0) {
      startFastFailChecker(maxInitTimeout)
    }
  }

  override def stop(): Unit = if (shutdown.compareAndSet(false, true)) {
    super.stop()
    lifetimeTerminatingChecker.foreach(checker => {
      val shutdownTimeout = conf.get(ENGINE_EXEC_POOL_SHUTDOWN_TIMEOUT)
      ThreadUtils.shutdown(
        checker,
        Duration(shutdownTimeout, TimeUnit.MILLISECONDS))
    })
    stopEngineExec.foreach(exec => {
      ThreadUtils.shutdown(
        exec,
        Duration(60, TimeUnit.SECONDS))
    })
  }

  def gracefulStop(): Unit = if (gracefulStopDeregistered.compareAndSet(false, true)) {
    val stopTask: Runnable = () => {
      if (!shutdown.get) {
        info(s"Spark engine is de-registering from engine discovery space.")
        frontendServices.flatMap(_.discoveryService).foreach(_.stop())
        while (backendService.sessionManager.getActiveUserSessionCount > 0) {
          Thread.sleep(TimeUnit.SECONDS.toMillis(10))
        }
        info(s"Spark engine has no open session now, terminating.")
        stop()
      }
    }
    stopEngineExec =
      Some(ThreadUtils.newDaemonFixedThreadPool(1, "spark-engine-graceful-stop"))
    stopEngineExec.get.execute(stopTask)
  }

  private[kyuubi] def startFastFailChecker(maxTimeout: Long): Unit = {
    val startedTime = System.currentTimeMillis()
    Utils.tryLogNonFatalError {
      ThreadUtils.runInNewThread("spark-engine-failfast-checker") {
        if (!shutdown.get) {
          while (backendService.sessionManager.getActiveUserSessionCount <= 0 &&
            System.currentTimeMillis() - startedTime < maxTimeout) {
            info(s"Waiting for the initial connection")
            Thread.sleep(Duration(10, TimeUnit.SECONDS).toMillis)
          }
          if (backendService.sessionManager.getActiveUserSessionCount <= 0) {
            error(s"Spark engine has been terminated because no incoming connection" +
              s" for more than $maxTimeout ms, de-registering from engine discovery space.")
            assert(currentEngine.isDefined)
            currentEngine.get.stop()
          }
        }
      }
    }
  }

  override protected def stopServer(): Unit = {
    countDownLatch.countDown()
  }

  private[kyuubi] def startLifetimeTerminatingChecker(stop: () => Unit): Unit = {
    val interval = conf.get(ENGINE_CHECK_INTERVAL)
    val maxLifetime = conf.get(ENGINE_SPARK_MAX_LIFETIME)
    val deregistered = new AtomicBoolean(false)
    if (maxLifetime > 0) {
      val gracefulPeriod = conf.get(ENGINE_SPARK_MAX_LIFETIME_GRACEFUL_PERIOD)
      val checkTask: Runnable = () => {
        val elapsedTime = System.currentTimeMillis() - getStartTime
        if (!shutdown.get && elapsedTime > maxLifetime) {
          if (deregistered.compareAndSet(false, true)) {
            info(s"Spark engine has been running for more than $maxLifetime ms," +
              s" deregistering from engine discovery space.")
            frontendServices.flatMap(_.discoveryService).foreach(_.stop())
          }

          if (backendService.sessionManager.getActiveUserSessionCount <= 0) {
            info(s"Spark engine has been running for more than $maxLifetime ms" +
              s" and no open session now, terminating.")
            stop()
          } else if (gracefulPeriod > 0 && elapsedTime > maxLifetime + gracefulPeriod) {
            backendService.sessionManager.allSessions().foreach { session =>
              val operationCount =
                backendService.sessionManager.operationManager.allOperations()
                  .filter(_.getSession == session)
                  .size
              if (operationCount == 0) {
                warn(s"Closing session ${session.handle.identifier} forcibly that has no" +
                  s" operation and has been running for more than $gracefulPeriod ms after engine" +
                  s" max lifetime.")
                try {
                  backendService.sessionManager.closeSession(session.handle)
                } catch {
                  case e: Throwable =>
                    error(s"Error closing session ${session.handle.identifier}", e)
                }
              }
            }
          }
        }
      }
      lifetimeTerminatingChecker =
        Some(ThreadUtils.newDaemonSingleThreadScheduledExecutor("spark-engine-lifetime-checker"))
      scheduleTolerableRunnableWithFixedDelay(
        lifetimeTerminatingChecker.get,
        checkTask,
        interval,
        interval,
        TimeUnit.MILLISECONDS)
    }
  }
}

object SparkGrpcEngine extends Logging {

  private var _sparkConf: SparkConf = _

  private var _kyuubiConf: KyuubiConf = _

  def kyuubiConf: KyuubiConf = _kyuubiConf

  var currentEngine: Option[SparkGrpcEngine] = None

  private lazy val user = currentUser

  private val countDownLatch = new CountDownLatch(1)

  private val sparkSessionCreated = new AtomicBoolean(false)

  // Kubernetes pod name max length - '-exec-' - Int.MAX_VALUE.length
  // 253 - 10 - 6
  val EXECUTOR_POD_NAME_PREFIX_MAX_LENGTH = 237

  SignalRegister.registerLogger(logger)
  setupConf()

  def setupConf(): Unit = {
    _sparkConf = new SparkConf()
    _kyuubiConf = KyuubiConf()
    _sparkConf.setIfMissing("spark.master", "local")

    val defaultCat = if (KyuubiSparkUtil.hiveClassesArePresent) "hive" else "in-memory"
    _sparkConf.setIfMissing("spark.sql.catalogImplementation", defaultCat)

    kyuubiConf.setIfMissing(FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
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
    kyuubiConf.unset(KyuubiReservedKeys.KYUUBI_ENGINE_CREDENTIALS_KEY)
    _sparkConf.set(s"spark.${KyuubiReservedKeys.KYUUBI_ENGINE_CREDENTIALS_KEY}", "")

    val session = SparkSession.builder.config(_sparkConf).getOrCreate

    KyuubiSparkUtil.initializeSparkSession(
      session,
      kyuubiConf.get(ENGINE_SPARK_INITIALIZE_SQL) ++ kyuubiConf.get(
        ENGINE_SESSION_SPARK_INITIALIZE_SQL))
    session.sparkContext.setLocalProperty(KYUUBI_ENGINE_URL, KyuubiSparkUtil.engineUrl)
    session
  }

  def startEngine(spark: SparkSession): Unit = {
    currentEngine = Some(new SparkGrpcEngine(spark))
    currentEngine.foreach { engine =>
      try {
        engine.initialize(kyuubiConf)
      } catch {
        case t: Throwable =>
          throw new KyuubiException(s"Failed to initialize SparkGrpcEngine: ${t.getMessage}", t)
      }
      try {
        engine.start()
      } catch {
        case t: Throwable =>
          throw new KyuubiException(s"Failed to start SparkGrpcEngine: ${t.getMessage}", t)
      }
      // Stop engine before SparkContext stopped to avoid calling a stopped SparkContext
      addShutdownHook(() => engine.stop(), SPARK_CONTEXT_SHUTDOWN_PRIORITY + 2)
    }
  }

  def main(args: Array[String]): Unit = {
    if (KyuubiSparkUtil.SPARK_ENGINE_RUNTIME_VERSION < "4.0") {
      throw new IllegalStateException(s"SparkGrocEngine requires Spark 4.0, " +
        s"but ${KyuubiSparkUtil.SPARK_ENGINE_RUNTIME_VERSION} is detected")
    }
    var spark: SparkSession = null
    try {
      spark = createSpark()
      sparkSessionCreated.set(true)
      try {
        startEngine(spark)
        // blocking main thread
        countDownLatch.await()
      } catch {
        case e: KyuubiException =>
          currentEngine match {
            case Some(engine) =>
              engine.stop()
              error("SparkGrpcEngine stopped abnormally", e)
            case _ => error("Current SparkGrpcEngine is not created.")
          }
          throw e
      }
    } catch {
      case i: InterruptedException if !sparkSessionCreated.get =>
        val msg = "The Engine main thread was interrupted, possibly due to `createSpark` timeout."
        error(msg, i)
        throw new InterruptedException(msg)
      case e: KyuubiException => throw e
      case t: Throwable =>
        error(s"Failed to instantiate SparkSession: ${t.getMessage}", t)
        throw t
    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}
