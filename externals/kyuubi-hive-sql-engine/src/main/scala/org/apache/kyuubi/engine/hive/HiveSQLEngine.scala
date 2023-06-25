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

import java.security.PrivilegedExceptionAction

import scala.util.control.NonFatal

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiReservedKeys}
import org.apache.kyuubi.engine.hive.HiveSQLEngine.currentEngine
import org.apache.kyuubi.engine.hive.events.{HiveEngineEvent, HiveEventHandlerRegister}
import org.apache.kyuubi.events.EventBus
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_CONN_RETRY_POLICY
import org.apache.kyuubi.ha.client.RetryPolicies
import org.apache.kyuubi.service.{AbstractBackendService, AbstractFrontendService, Serverable, ServiceState}
import org.apache.kyuubi.util.SignalRegister

class HiveSQLEngine extends Serverable("HiveSQLEngine") {
  override val backendService: AbstractBackendService = new HiveBackendService(this)
  override val frontendServices: Seq[AbstractFrontendService] =
    Seq(new HiveTBinaryFrontendService(this))
  private[hive] val engineStartTime = System.currentTimeMillis()

  override def start(): Unit = {
    super.start()
    // Start engine self-terminating checker after all services are ready and it can be reached by
    // all servers in engine spaces.
    backendService.sessionManager.startTerminatingChecker(() => stop())
  }

  override protected def stopServer(): Unit = {
    currentEngine.foreach { engine =>
      val event = HiveEngineEvent(engine)
        .copy(state = ServiceState.STOPPED, endTime = System.currentTimeMillis())
      EventBus.post(event)
    }

    // #2351
    // https://issues.apache.org/jira/browse/HIVE-23164
    // Server is not properly terminated because of non-daemon threads
    System.exit(0)
  }
}

object HiveSQLEngine extends Logging {
  var currentEngine: Option[HiveSQLEngine] = None
  val hiveConf = new HiveConf()
  val kyuubiConf = new KyuubiConf()

  def startEngine(): Unit = {
    try {
      // TODO: hive 2.3.x has scala 2.11 deps.
      initLoggerEventHandler(kyuubiConf)
    } catch {
      case NonFatal(e) =>
        warn(s"Failed to initialize Logger EventHandler: ${e.getMessage}", e)
    }
    kyuubiConf.setIfMissing(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    kyuubiConf.setIfMissing(HA_ZK_CONN_RETRY_POLICY, RetryPolicies.N_TIME.toString)

    for ((k, v) <- kyuubiConf.getAll) {
      hiveConf.set(k, v)
    }

    val isEmbeddedMetaStore = {
      val msUri = hiveConf.get("hive.metastore.uris")
      val msConnUrl = hiveConf.get("javax.jdo.option.ConnectionURL")
      (msUri == null || msUri.trim().isEmpty) &&
      (msConnUrl != null && msConnUrl.startsWith("jdbc:derby"))
    }
    if (isEmbeddedMetaStore) {
      hiveConf.setBoolean("hive.metastore.schema.verification", false)
      hiveConf.setBoolean("datanucleus.schema.autoCreateAll", true)
      hiveConf.set(
        "hive.metastore.warehouse.dir",
        Utils.createTempDir(prefix = "kyuubi_hive_warehouse").toString)
      hiveConf.set("hive.metastore.fastpath", "true")
    }

    val engine = new HiveSQLEngine()
    info(s"Starting ${engine.getName}")
    engine.initialize(kyuubiConf)
    EventBus.post(HiveEngineEvent(engine))
    engine.start()
    val event = HiveEngineEvent(engine)
    info(event)
    EventBus.post(event)
    Utils.addShutdownHook(() => {
      engine.getServices.foreach(_.stop())
    })
    currentEngine = Some(engine)
  }

  private def initLoggerEventHandler(conf: KyuubiConf): Unit = {
    HiveEventHandlerRegister.registerEventLoggers(conf)
  }

  def main(args: Array[String]): Unit = {
    SignalRegister.registerLogger(logger)
    try {
      Utils.fromCommandLineArgs(args, kyuubiConf)
      val sessionUser = kyuubiConf.getOption(KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY)
      val realUser = UserGroupInformation.getLoginUser

      if (sessionUser.isEmpty || sessionUser.get == realUser.getShortUserName) {
        startEngine()
      } else {
        val effectiveUser = UserGroupInformation.createProxyUser(sessionUser.get, realUser)
        effectiveUser.doAs(new PrivilegedExceptionAction[Unit] {
          override def run(): Unit = startEngine()
        })
      }

    } catch {
      case t: Throwable => currentEngine match {
          case Some(engine) =>
            engine.stop()
            val event = HiveEngineEvent(engine)
              .copy(endTime = System.currentTimeMillis(), diagnostic = t.getMessage)
            EventBus.post(event)
          case _ =>
            error(s"Failed to start Hive SQL engine: ${t.getMessage}.", t)
        }
    }
  }
}
