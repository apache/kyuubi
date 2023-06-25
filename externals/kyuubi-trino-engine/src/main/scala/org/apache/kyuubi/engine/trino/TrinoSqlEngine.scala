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

package org.apache.kyuubi.engine.trino

import java.util.concurrent.CountDownLatch

import scala.util.control.NonFatal

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.Utils.{addShutdownHook, TRINO_ENGINE_SHUTDOWN_PRIORITY}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.trino.TrinoSqlEngine.{countDownLatch, currentEngine}
import org.apache.kyuubi.engine.trino.event.{TrinoEngineEvent, TrinoEventHandlerRegister}
import org.apache.kyuubi.events.EventBus
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_CONN_RETRY_POLICY
import org.apache.kyuubi.ha.client.RetryPolicies
import org.apache.kyuubi.service.Serverable
import org.apache.kyuubi.util.SignalRegister

case class TrinoSqlEngine()
  extends Serverable("TrinoSQLEngine") {

  override val backendService = new TrinoBackendService()

  override val frontendServices = Seq(new TrinoTBinaryFrontendService(this))

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

object TrinoSqlEngine extends Logging {
  private val countDownLatch = new CountDownLatch(1)

  val kyuubiConf: KyuubiConf = KyuubiConf()

  var currentEngine: Option[TrinoSqlEngine] = None

  def startEngine(): Unit = {
    try {
      initLoggerEventHandler(kyuubiConf)
    } catch {
      case NonFatal(e) =>
        warn(s"Failed to initialize Logger EventHandler: ${e.getMessage}", e)
    }
    currentEngine = Some(new TrinoSqlEngine())
    currentEngine.foreach { engine =>
      engine.initialize(kyuubiConf)
      EventBus.post(TrinoEngineEvent(engine))
      engine.start()
      EventBus.post(TrinoEngineEvent(engine))
      addShutdownHook(
        () => {
          engine.stop()
          val event = TrinoEngineEvent(engine)
            .copy(endTime = System.currentTimeMillis())
          EventBus.post(event)
        },
        TRINO_ENGINE_SHUTDOWN_PRIORITY + 1)
    }
  }

  private def initLoggerEventHandler(conf: KyuubiConf): Unit = {
    TrinoEventHandlerRegister.registerEventLoggers(conf)
  }

  def main(args: Array[String]): Unit = {
    SignalRegister.registerLogger(logger)

    try {
      Utils.fromCommandLineArgs(args, kyuubiConf)
      kyuubiConf.setIfMissing(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
      kyuubiConf.setIfMissing(HA_ZK_CONN_RETRY_POLICY, RetryPolicies.N_TIME.toString)

      startEngine()
      // blocking main thread
      countDownLatch.await()
    } catch {
      case t: Throwable if currentEngine.isDefined =>
        currentEngine.foreach { engine =>
          error(t)
          engine.stop()
          val event = TrinoEngineEvent(engine)
            .copy(endTime = System.currentTimeMillis(), diagnostic = t.getMessage)
          EventBus.post(event)
        }
      case t: Throwable => error("Create Trino Engine Failed", t)
    }
  }
}
