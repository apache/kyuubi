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

import org.apache.spark.SparkConf
import org.apache.spark.kyuubi.SparkSQLEngineListener
import org.apache.spark.kyuubi.ui.EngineTab
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.Logging
import org.apache.kyuubi.Utils._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.SparkSQLEngine.countDownLatch
import org.apache.kyuubi.engine.spark.events.{EngineEvent, EventLoggingService}
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.{EngineServiceDiscovery, RetryPolicies, ServiceDiscovery}
import org.apache.kyuubi.service.{Serverable, Service, ServiceState, ThriftFrontendService}
import org.apache.kyuubi.util.SignalRegister

case class SparkSQLEngine(spark: SparkSession) extends Serverable("SparkSQLEngine") {

  lazy val engineStatus: EngineEvent = EngineEvent(this)

  private val OOMHook = new Runnable { override def run(): Unit = stop() }
  private val eventLogging = new EventLoggingService(this)
  override val backendService = new SparkSQLBackendService(spark)
  val frontendService = new ThriftFrontendService(backendService, OOMHook)
  override val discoveryService: Service = new EngineServiceDiscovery(this)

  override protected def supportsServiceDiscovery: Boolean = {
    ServiceDiscovery.supportServiceDiscovery(conf)
  }

  override def initialize(conf: KyuubiConf): Unit = {
    val listener = new SparkSQLEngineListener(this)
    spark.sparkContext.addSparkListener(listener)
    addService(eventLogging)
    addService(frontendService)
    super.initialize(conf)
    eventLogging.onEvent(engineStatus.copy(state = ServiceState.INITIALIZED.id))
  }

  override def start(): Unit = {
    super.start()
    // Start engine self-terminating checker after all services are ready and it can be reached by
    // all servers in engine spaces.
    backendService.sessionManager.startTerminatingChecker()
    eventLogging.onEvent(engineStatus.copy(state = ServiceState.STARTED.id))
  }

  override def stop(): Unit = {
    eventLogging.onEvent(
      engineStatus.copy(state = ServiceState.STOPPED.id, endTime = System.currentTimeMillis()))
    super.stop()
  }

  override protected def stopServer(): Unit = {
    countDownLatch.countDown()
  }

  override def connectionUrl: String = frontendService.connectionUrl()

  def engineId: String = {
    spark.sparkContext.applicationAttemptId.getOrElse(spark.sparkContext.applicationId)
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
      engine.initialize(kyuubiConf)
      engine.start()
      // Stop engine before SparkContext stopped to avoid calling a stopped SparkContext
      addShutdownHook(() => engine.stop(), SPARK_CONTEXT_SHUTDOWN_PRIORITY + 1)
      EngineTab(engine)
      info(engine.engineStatus)
    }
  }

  def main(args: Array[String]): Unit = {
    SignalRegister.registerLogger(logger)
    var spark: SparkSession = null
    try {
      spark = createSpark()
      startEngine(spark)
      // blocking main thread
      countDownLatch.await()
    } catch {
      case t: Throwable if currentEngine.isDefined =>
        currentEngine.foreach { engine =>
          val status =
            engine.engineStatus.copy(diagnostic = s"Error State SparkSQL Engine ${t.getMessage}")
          EventLoggingService.onEvent(status)
          error(status, t)
          engine.stop()
        }
      case t: Throwable =>
        error("Create SparkSQL Engine Failed", t)
    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}
