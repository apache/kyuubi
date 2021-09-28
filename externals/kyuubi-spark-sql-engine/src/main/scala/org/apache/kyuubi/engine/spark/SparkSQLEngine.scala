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
import java.util.concurrent.CountDownLatch

import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.kyuubi.SparkSQLEngineListener
import org.apache.spark.kyuubi.ui.EngineTab
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.Utils._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.SparkSQLEngine.countDownLatch
import org.apache.kyuubi.engine.spark.events.{EngineEvent, EventLoggingService}
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.RetryPolicies
import org.apache.kyuubi.service.Serverable
import org.apache.kyuubi.util.SignalRegister

case class SparkSQLEngine(spark: SparkSession) extends Serverable("SparkSQLEngine") {

  override val backendService = new SparkSQLBackendService(spark)
  override val frontendServices = Seq(new SparkThriftBinaryFrontendService(this))

  override def initialize(conf: KyuubiConf): Unit = {
    val listener = new SparkSQLEngineListener(this)
    spark.sparkContext.addSparkListener(listener)
    super.initialize(conf)
  }

  override def start(): Unit = {
    super.start()
    // Start engine self-terminating checker after all services are ready and it can be reached by
    // all servers in engine spaces.
    backendService.sessionManager.startTerminatingChecker()
  }

  override protected def stopServer(): Unit = {
    countDownLatch.countDown()
  }
}

object SparkSQLEngine extends Logging {

  val kyuubiConf: KyuubiConf = KyuubiConf()

  var currentEngine: Option[SparkSQLEngine] = None

  private val user = currentUser

  private val countDownLatch = new CountDownLatch(1)

  def createSpark(): SparkSession = {
    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.sql.execution.topKSortFallbackThreshold", "10000")
    sparkConf.setIfMissing("spark.sql.legacy.castComplexTypesToString.enabled", "true")
    sparkConf.setIfMissing("spark.master", "local")
    sparkConf.setIfMissing("spark.ui.port", "0")

    val appName = s"kyuubi_${user}_spark_${Instant.now}"
    sparkConf.setIfMissing("spark.app.name", appName)
    val defaultCat = if (KyuubiSparkUtil.hiveClassesArePresent) "hive" else "in-memory"
    sparkConf.setIfMissing("spark.sql.catalogImplementation", defaultCat)

    kyuubiConf.setIfMissing(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    kyuubiConf.setIfMissing(HA_ZK_CONN_RETRY_POLICY, RetryPolicies.N_TIME.toString)

    // Pass kyuubi config from spark with `spark.kyuubi`
    val sparkToKyuubiPrefix = "spark.kyuubi."
    sparkConf.getAllWithPrefix(sparkToKyuubiPrefix).foreach { case (k, v) =>
      kyuubiConf.set(s"kyuubi.$k", v)
    }

    if (logger.isDebugEnabled) {
      kyuubiConf.getAll.foreach { case (k, v) =>
        debug(s"KyuubiConf: $k = $v")
      }
    }

    val session = SparkSession.builder.config(sparkConf).getOrCreate
    (kyuubiConf.get(ENGINE_INITIALIZE_SQL) ++ kyuubiConf.get(ENGINE_SESSION_INITIALIZE_SQL))
      .foreach { sqlStr =>
        session.sparkContext.setJobGroup(appName, sqlStr, interruptOnCancel = true)
        debug(s"Execute session initializing sql: $sqlStr")
        session.sql(sqlStr).isEmpty
      }
    session
  }

  def startEngine(spark: SparkSession): Unit = {
    currentEngine = Some(new SparkSQLEngine(spark))
    currentEngine.foreach { engine =>
      // start event logging ahead so that we can capture all statuses
      val eventLogging = new EventLoggingService(spark.sparkContext)
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
        EventLoggingService.onEvent(EngineEvent(engine))
      } catch {
        case t: Throwable =>
          throw new KyuubiException(s"Failed to initialize SparkSQLEngine: ${t.getMessage}", t)
      }
      try {
        engine.start()
        EngineTab(engine)
        val event = EngineEvent(engine)
        info(event)
        EventLoggingService.onEvent(event)
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
    SignalRegister.registerLogger(logger)
    var spark: SparkSession = null
    try {
      spark = createSpark()
      try {
        startEngine(spark)
        // blocking main thread
        countDownLatch.await()
      } catch {
        case e: KyuubiException if currentEngine.isDefined =>
          val engine = currentEngine.get
          engine.stop()
          val event = EngineEvent(engine).copy(diagnostic = e.getMessage)
          EventLoggingService.onEvent(event)
          error(event, e)
      }
    } catch {
      case t: Throwable => error(s"Failed to instantiate SparkSession: ${t.getMessage}", t)
    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}
