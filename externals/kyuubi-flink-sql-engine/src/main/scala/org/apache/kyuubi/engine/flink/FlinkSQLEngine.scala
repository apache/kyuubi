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

package org.apache.kyuubi.engine.flink

import java.util.concurrent.CountDownLatch

import org.apache.flink.table.client.gateway.context.DefaultContext
import org.apache.flink.table.client.gateway.local.LocalContextUtils

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.Utils.{addShutdownHook, DEFAULT_SHUTDOWN_PRIORITY}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.flink.FlinkSQLEngine.{countDownLatch, currentEngine}
import org.apache.kyuubi.engine.flink.result.EngineEnvironmentUtil
import org.apache.kyuubi.service.Serverable
import org.apache.kyuubi.util.SignalRegister

/**
 * A flink sql engine just like an instance of Flink SQL Engine.
 */
case class FlinkSQLEngine(engineContext: DefaultContext) extends Serverable("FlinkSQLEngine") {

  override val backendService = new FlinkSQLBackendService(engineContext)
  override val frontendServices = Seq(new FlinkThriftBinaryFrontendService(this))

  override def start(): Unit = {
    super.start()
    backendService.sessionManager.startTerminatingChecker(() => {
      assert(currentEngine.isDefined)
      currentEngine.get.stop()
    })
  }

  override protected def stopServer(): Unit = {
    countDownLatch.countDown()
  }
}

object FlinkSQLEngine extends Logging {

  val kyuubiConf: KyuubiConf = KyuubiConf()
  var currentEngine: Option[FlinkSQLEngine] = None

  private val countDownLatch = new CountDownLatch(1)

  def main(args: Array[String]): Unit = {
    SignalRegister.registerLogger(logger)

    EngineEnvironmentUtil.checkFlinkVersion()

    try {
      val cliOptions = EngineEnvironmentUtil.parseCliOptions(args)

      val defaultContext = LocalContextUtils.buildDefaultContext(cliOptions)

      kyuubiConf.setIfMissing(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

      startEngine(defaultContext)

      // blocking main thread
      countDownLatch.await()
    } catch {
      case t: Throwable if currentEngine.isDefined =>
        val engine = currentEngine.get
        error(t)
        engine.stop()
      case t: Throwable =>
        error("Failed to launch Flink SQL Engine process: ", t)
    }
  }

  def startEngine(engineContext: DefaultContext): Unit = {
    currentEngine = Some(new FlinkSQLEngine(engineContext))
    currentEngine.foreach { engine =>
      try {
        engine.initialize(kyuubiConf)
      } catch {
        case t: Throwable =>
          throw new KyuubiException(s"Failed to initialize FlinkSQLEngine: ${t.getMessage}", t)
      }

      try {
        engine.start()
      } catch {
        case t: Throwable =>
          throw new KyuubiException(s"Failed to start FlinkSQLEngine: ${t.getMessage}", t)
      }

      addShutdownHook(() => engine.stop(), DEFAULT_SHUTDOWN_PRIORITY + 1)
    }
  }

}
