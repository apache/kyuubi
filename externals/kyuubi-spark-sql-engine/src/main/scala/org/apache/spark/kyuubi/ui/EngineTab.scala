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

package org.apache.spark.kyuubi.ui

import javax.servlet.http.HttpServletRequest

import org.apache.spark.ui.SparkUITab
import scala.util.control.NonFatal

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.SparkSQLEngine
import org.apache.kyuubi.service.ServiceState

/**
 * Note that [[SparkUITab]] is private for Spark
 */
case class EngineTab(engine: SparkSQLEngine)
  extends SparkUITab(engine.spark.sparkContext.ui.orNull, "kyuubi") with Logging {

  override val name: String = "Kyuubi Query Engine"
  val killEnabled = engine.getConf.get(KyuubiConf.ENGINE_UI_STOP_ENABLED)

  engine.spark.sparkContext.ui.foreach { ui =>
    this.attachPage(EnginePage(this))
    ui.attachTab(this)
    Utils.addShutdownHook(() => ui.detachTab(this))
  }

  engine.spark.sparkContext.ui.foreach { ui =>
    try {
      // Spark shade the jetty package so here we use reflect
      Class.forName("org.apache.spark.ui.SparkUI")
        .getMethod("attachHandler",
          classOf[org.sparkproject.jetty.servlet.ServletContextHandler])
        .invoke(ui, KyuubiUIUtils.createRedirectHandler(
          "/kyuubi/stop", "/kyuubi", handleKillRequest, httpMethods = Set("GET", "POST")))
    } catch {
      case NonFatal(e) =>
        warn("Failed to attach handler using SparkUI, please check the Spark version. " +
          s"So the config '${KyuubiConf.ENGINE_UI_STOP_ENABLED.key}' does not work.", e)
    }
  }

  def handleKillRequest(request: HttpServletRequest): Unit = {
    if (killEnabled && engine != null && engine.getServiceState != ServiceState.STOPPED) {
      engine.stop()
    }
  }
}
