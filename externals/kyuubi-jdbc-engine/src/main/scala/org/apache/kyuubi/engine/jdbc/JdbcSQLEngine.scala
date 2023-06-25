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
package org.apache.kyuubi.engine.jdbc

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.Utils.{addShutdownHook, JDBC_ENGINE_SHUTDOWN_PRIORITY}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_JDBC_INITIALIZE_SQL
import org.apache.kyuubi.engine.jdbc.JdbcSQLEngine.currentEngine
import org.apache.kyuubi.engine.jdbc.util.KyuubiJdbcUtils
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_CONN_RETRY_POLICY
import org.apache.kyuubi.ha.client.RetryPolicies
import org.apache.kyuubi.service.Serverable
import org.apache.kyuubi.util.SignalRegister

class JdbcSQLEngine extends Serverable("JdbcSQLEngine") {

  override val backendService = new JdbcBackendService()
  override val frontendServices =
    Seq(new JdbcTBinaryFrontendService(this))

  override def start(): Unit = {
    super.start()
    // Start engine self-terminating checker after all services are ready and it can be reached by
    // all servers in engine spaces.
    backendService.sessionManager.startTerminatingChecker(() => {
      currentEngine.foreach(_.stop())
    })
  }

  override protected def stopServer(): Unit = {}
}

object JdbcSQLEngine extends Logging {

  val kyuubiConf: KyuubiConf = KyuubiConf()

  var currentEngine: Option[JdbcSQLEngine] = None

  def startEngine(): Unit = {
    currentEngine = Some(new JdbcSQLEngine())
    currentEngine.foreach { engine =>
      engine.initialize(kyuubiConf)
      engine.start()
      addShutdownHook(
        () => {
          engine.stop()
        },
        JDBC_ENGINE_SHUTDOWN_PRIORITY + 1)
    }
  }

  def main(args: Array[String]): Unit = {
    SignalRegister.registerLogger(logger)

    try {
      Utils.fromCommandLineArgs(args, kyuubiConf)
      kyuubiConf.setIfMissing(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
      kyuubiConf.setIfMissing(HA_ZK_CONN_RETRY_POLICY, RetryPolicies.N_TIME.toString)

      startEngine()

      KyuubiJdbcUtils.initializeJdbcSession(kyuubiConf, kyuubiConf.get(ENGINE_JDBC_INITIALIZE_SQL))
    } catch {
      case t: Throwable if currentEngine.isDefined =>
        currentEngine.foreach { engine =>
          engine.stop()
        }
        error("Failed to create Jdbc Engine.", t)
        throw t
      case t: Throwable =>
        error("Failed to create Jdbc Engine.", t)
        throw t
    }
  }
}
