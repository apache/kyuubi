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
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.SparkSQLEngine.countDownLatch
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.{RetryPolicies, ServiceDiscovery}
import org.apache.kyuubi.service.Serverable
import org.apache.kyuubi.util.SignalRegister

private[spark] final class SparkSQLEngine(name: String, spark: SparkSession)
  extends Serverable(name) {

  def this(spark: SparkSession) = this(classOf[SparkSQLEngine].getSimpleName, spark)

  override private[kyuubi] val backendService = new SparkSQLBackendService(spark)
  private val discoveryService = new ServiceDiscovery(this)

  override def initialize(conf: KyuubiConf): Unit = {
    val listener = new SparkSQLEngineListener(this)
    spark.sparkContext.addSparkListener(listener)
    super.initialize(conf)
    if (ServiceDiscovery.supportServiceDiscovery(conf)) {
      addService(discoveryService)
      discoveryService.initialize(conf)
    }
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

  private val user = Utils.currentUser

  private val countDownLatch = new CountDownLatch(1)

  def createSpark(): SparkSession = {
    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local")
    sparkConf.setIfMissing("spark.ui.port", "0")

    val appName = s"kyuubi_${user}_spark_${Instant.now}"
    sparkConf.setIfMissing("spark.app.name", appName)

    kyuubiConf.setIfMissing(KyuubiConf.FRONTEND_BIND_PORT, 0)
    kyuubiConf.setIfMissing(HA_ZK_CONN_RETRY_POLICY, RetryPolicies.N_TIME.toString)

    val prefix = "spark.kyuubi."

    sparkConf.getAllWithPrefix(prefix).foreach { case (k, v) =>
      kyuubiConf.set(s"kyuubi.$k", v)
    }

    if (logger.isDebugEnabled) {
      kyuubiConf.getAll.foreach { case (k, v) =>
        debug(s"KyuubiConf: $k = $v")
      }
    }

    val session = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    session.sql("SHOW DATABASES")
    session
  }

  def startEngine(spark: SparkSession): SparkSQLEngine = {
    val engine = new SparkSQLEngine(spark)
    engine.initialize(kyuubiConf)
    engine.start()
    sys.addShutdownHook(engine.stop())
    currentEngine = Some(engine)
    engine
  }

  def main(args: Array[String]): Unit = {
    SignalRegister.registerLogger(logger)
    var spark: SparkSession = null
    var engine: SparkSQLEngine = null
    try {
      spark = createSpark()
      engine = startEngine(spark)
      info(KyuubiSparkUtil.diagnostics(spark))
      // blocking main thread
      countDownLatch.await()
    } catch {
      case t: Throwable =>
        error("Error start SparkSQLEngine", t)
        if (engine != null) {
          engine.stop()
        }
    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}
