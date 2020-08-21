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

import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.{CompositeService, FrontendService}
import org.apache.kyuubi.util.SignalRegister

class SparkSQLEngine(name: String, spark: SparkSession) extends CompositeService(name) {

  private val started = new AtomicBoolean(false)
  private val OOMHook = new Runnable { override def run(): Unit = stop() }
  private val backendService = new SparkSQLBackendService(spark)
  private val frontendService = new FrontendService(backendService, OOMHook)

  def connectionUrl: String = frontendService.connectionUrl

  def this(spark: SparkSession) = this(classOf[SparkSQLEngine].getSimpleName, spark)

  override def initialize(conf: KyuubiConf): Unit = {
    addService(backendService)
    addService(frontendService)
    super.initialize(conf)
  }

  override def start(): Unit = {
    super.start()
    started.set(true)
  }

  override def stop(): Unit = {
    try {
      spark.stop()
    } catch {
      case t: Throwable =>
        warn(s"Error stopping spark ${t.getMessage}", t)
    } finally {
      if (started.getAndSet(false)) {
        super.stop()
      }
    }
  }
}


private object SparkSQLEngine extends Logging {

  val kyuubiConf: KyuubiConf = KyuubiConf()

  def createSpark(): SparkSession = {
    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local")
    sparkConf.setIfMissing("spark.ui.port", "0")

    val appName = Seq(
      "kyuubi",
      Utils.currentUser,
      classOf[SparkSQLEngine].getSimpleName,
      LocalDateTime.now).mkString("_")

    sparkConf.setAppName(appName)

    kyuubiConf.setIfMissing(KyuubiConf.FRONTEND_BIND_PORT, 0)

    val prefix = "spark.kyuubi."

    sparkConf.getAllWithPrefix(prefix).foreach { case (k, v) =>
      kyuubiConf.set(s"kyuubi.$k", v)
    }

    if (logger.isDebugEnabled) {
      kyuubiConf.getAll.foreach { case (k, v) =>
        debug(s"KyuubiConf: $k = $v")
      }
    }

    SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
  }


  def startEngine(spark: SparkSession): SparkSQLEngine = {
    val engine = new SparkSQLEngine(spark)
    engine.initialize(kyuubiConf)
    engine.start()
    sys.addShutdownHook(engine.stop())
    engine
  }

  def main(args: Array[String]): Unit = {
    SignalRegister.registerLogger(logger)

    var spark: SparkSession = null
    var engine: SparkSQLEngine = null
    try {
      spark = createSpark()
      engine = startEngine(spark)
    } catch {
      case t: Throwable =>
        error("Error start SparkSQLEngine", t)
        if (engine != null) {
          engine.stop()
        } else if (spark != null) {
          spark.stop()
        }
    }
  }
}
