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

package org.apache.kyuubi.engine.hive

import scala.util.control.NonFatal

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.hive.event.{HiveEngineEvent, HiveEventLoggingService}
import org.apache.kyuubi.events.EventLogging
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_CONN_RETRY_POLICY
import org.apache.kyuubi.ha.client.RetryPolicies
import org.apache.kyuubi.service.{AbstractBackendService, AbstractFrontendService, Serverable}
import org.apache.kyuubi.util.SignalRegister

class HiveSQLEngine extends Serverable("HiveSQLEngine") {
  override val backendService: AbstractBackendService = new HiveBackendService(this)
  override val frontendServices: Seq[AbstractFrontendService] =
    Seq(new HiveTBinaryFrontendService(this))

  override def start(): Unit = {
    super.start()
    // Start engine self-terminating checker after all services are ready and it can be reached by
    // all servers in engine spaces.
    backendService.sessionManager.startTerminatingChecker(() => stop())
  }

  override protected def stopServer(): Unit = {}
}

object HiveSQLEngine extends Logging {
  var currentEngine: Option[HiveSQLEngine] = None
  val hiveConf = new HiveConf()
  val kyuubiConf = new KyuubiConf()

  def startEngine(): HiveSQLEngine = {
    val eventLogging = new HiveEventLoggingService()
    try {
      eventLogging.initialize(kyuubiConf)
      eventLogging.start()
    } catch {
      case NonFatal(e) =>
        warn(s"Failed to initialize EventLoggingService: ${e.getMessage}", e)
    }
    kyuubiConf.setIfMissing(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    kyuubiConf.setIfMissing(HA_ZK_CONN_RETRY_POLICY, RetryPolicies.N_TIME.toString)

    for ((k, v) <- kyuubiConf.getAll) {
      hiveConf.set(k, v)
    }

    val isEmbeddedMetaStore = {
      val msUri = hiveConf.getVar(ConfVars.METASTOREURIS)
      val msConnUrl = hiveConf.getVar(ConfVars.METASTORECONNECTURLKEY)
      (msUri == null || msUri.trim().isEmpty) &&
      (msConnUrl != null && msConnUrl.startsWith("jdbc:derby"))
    }
    if (isEmbeddedMetaStore) {
      hiveConf.setBoolean("hive.metastore.schema.verification", false)
      hiveConf.setBoolean("datanucleus.schema.autoCreateAll", true)
      hiveConf.set(
        "hive.metastore.warehouse.dir",
        Utils.createTempDir(namePrefix = "kyuubi_hive_warehouse").toString)
    }

    val engine = new HiveSQLEngine()
    info(s"Starting ${engine.getName}")
    engine.initialize(kyuubiConf)
    EventLogging.onEvent(HiveEngineEvent(engine))
    engine.start()
    val event = HiveEngineEvent(engine)
    info(event)
    EventLogging.onEvent(event)
    Utils.addShutdownHook(() => engine.stop())
    currentEngine = Some(engine)
    engine
  }

  def main(args: Array[String]): Unit = {
    SignalRegister.registerLogger(logger)
    try {
      startEngine()
    } catch {
      case t: Throwable => currentEngine match {
        case Some(engine) =>
          engine.stop()
          val event = HiveEngineEvent(engine).copy(diagnostic = t.getMessage)
          EventLogging.onEvent(event)
        case _ =>
          error(t)
      }
    }
  }
}
