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

package org.apache.spark.ui

import javax.servlet.http.HttpServletRequest

import scala.util.control.NonFatal

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.SparkSQLEngine
import org.apache.kyuubi.engine.spark.events.EngineEventsStore
import org.apache.kyuubi.service.ServiceState
import org.apache.kyuubi.util.ClassUtils

/**
 * Note that [[SparkUITab]] is private for Spark
 */
case class EngineTab(
    engine: Option[SparkSQLEngine],
    sparkUI: Option[SparkUI],
    store: EngineEventsStore,
    kyuubiConf: KyuubiConf)
  extends SparkUITab(sparkUI.orNull, "kyuubi") with Logging {

  override val name: String = "Kyuubi Query Engine"
  val killEnabled = kyuubiConf.get(KyuubiConf.ENGINE_UI_STOP_ENABLED)

  val startTime = engine.map(_.getStartTime).getOrElse {
    sparkUI
      .map(ui => ui.store.applicationInfo().attempts.head.startTime.getTime)
      .getOrElse(0L)
  }

  def endTime(): Long = engine.map(_ => System.currentTimeMillis()).getOrElse {
    sparkUI
      .map(ui => ui.store.applicationInfo().attempts.head.endTime.getTime)
      .getOrElse(0L)
  }

  sparkUI.foreach { ui =>
    this.attachPage(EnginePage(this))
    this.attachPage(EngineSessionPage(this))
    ui.attachTab(this)
    Utils.addShutdownHook(() => ui.detachTab(this))
  }

  sparkUI.foreach { ui =>
    try {
      // Spark shade the jetty package so here we use reflection
      val sparkServletContextHandlerClz = loadSparkServletContextHandler
      val attachHandlerMethod = Class.forName("org.apache.spark.ui.SparkUI")
        .getMethod("attachHandler", sparkServletContextHandlerClz)
      val createRedirectHandlerMethod = Class.forName("org.apache.spark.ui.JettyUtils")
        .getMethod(
          "createRedirectHandler",
          classOf[String],
          classOf[String],
          classOf[(HttpServletRequest) => Unit],
          classOf[String],
          classOf[Set[String]])

      attachHandlerMethod
        .invoke(
          ui,
          createRedirectHandlerMethod
            .invoke(null, "/kyuubi/stop", "/kyuubi", handleKillRequest _, "", Set("GET", "POST")))

      attachHandlerMethod
        .invoke(
          ui,
          createRedirectHandlerMethod
            .invoke(
              null,
              "/kyuubi/gracefulstop",
              "/kyuubi",
              handleGracefulKillRequest _,
              "",
              Set("GET", "POST")))
    } catch {
      case NonFatal(cause) => reportInstallError(cause)
      case cause: NoClassDefFoundError => reportInstallError(cause)
    }
  }

  private def reportInstallError(cause: Throwable): Unit = {
    warn(
      "Failed to attach handler using SparkUI, please check the Spark version. " +
        s"So the config '${KyuubiConf.ENGINE_UI_STOP_ENABLED.key}' does not work.",
      cause)
  }

  private def loadSparkServletContextHandler: Class[_] = {
    // [KYUUBI #3627]: the official spark release uses the shaded and relocated jetty classes,
    // but if use sbt to build for testing, e.g. docker image, it still uses vanilla jetty classes.
    val shaded = "org.sparkproject.jetty.servlet.ServletContextHandler"
    val vanilla = "org.eclipse.jetty.servlet.ServletContextHandler"
    if (ClassUtils.classIsLoadable(shaded)) {
      Class.forName(shaded)
    } else {
      Class.forName(vanilla)
    }
  }

  def handleKillRequest(request: HttpServletRequest): Unit = {
    if (killEnabled && engine.isDefined && engine.get.getServiceState != ServiceState.STOPPED) {
      engine.get.stop()
    }
  }

  def handleGracefulKillRequest(request: HttpServletRequest): Unit = {
    if (killEnabled && engine.isDefined && engine.get.getServiceState != ServiceState.STOPPED) {
      engine.get.gracefulStop()
    }
  }
}
