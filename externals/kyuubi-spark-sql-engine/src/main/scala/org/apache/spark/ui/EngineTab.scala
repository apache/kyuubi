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

import scala.util.control.NonFatal

import net.bytebuddy.ByteBuddy
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy
import net.bytebuddy.implementation.MethodCall
import net.bytebuddy.matcher.ElementMatchers.{isConstructor, named}
import org.apache.spark.util.{Utils => SparkUtils}

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil.SPARK_ENGINE_RUNTIME_VERSION
import org.apache.kyuubi.engine.spark.SparkSQLEngine
import org.apache.kyuubi.engine.spark.events.EngineEventsStore
import org.apache.kyuubi.service.ServiceState
import org.apache.kyuubi.util.reflect.{DynClasses, DynMethods}

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

  private val enginePage = {
    val dispatchMethod = classOf[EnginePage].getMethod("dispatchRender", classOf[AnyRef])
    new ByteBuddy()
      .subclass(classOf[EnginePage], ConstructorStrategy.Default.IMITATE_SUPER_CLASS_PUBLIC)
      .method(isConstructor()).intercept(MethodCall.invokeSuper())
      .method(named("render")).intercept(MethodCall.invoke(dispatchMethod).withAllArguments())
      .make()
      .load(SparkUtils.getContextOrSparkClassLoader, ClassLoadingStrategy.Default.INJECTION)
      .getLoaded
      .getDeclaredConstructor(classOf[EngineTab])
      .newInstance(this)
  }

  private val engineSessionPage = {
    val dispatchMethod = classOf[EngineSessionPage].getMethod("dispatchRender", classOf[AnyRef])
    new ByteBuddy()
      .subclass(classOf[EngineSessionPage], ConstructorStrategy.Default.IMITATE_SUPER_CLASS_PUBLIC)
      .method(isConstructor()).intercept(MethodCall.invokeSuper())
      .method(named("render")).intercept(MethodCall.invoke(dispatchMethod).withAllArguments())
      .make()
      .load(SparkUtils.getContextOrSparkClassLoader, ClassLoadingStrategy.Default.INJECTION)
      .getLoaded
      .getDeclaredConstructor(classOf[EngineTab])
      .newInstance(this)
  }

  sparkUI.foreach { ui =>
    this.attachPage(enginePage)
    this.attachPage(engineSessionPage)
    ui.attachTab(this)
    Utils.addShutdownHook(() => ui.detachTab(this))
  }

  sparkUI.foreach { ui =>
    try {
      val sparkServletContextHandlerClz = DynClasses.builder()
        // for official Spark releases and distributions built via Maven
        .impl("org.sparkproject.jetty.servlet.ServletContextHandler")
        // for distributions built via SBT
        .impl("org.eclipse.jetty.servlet.ServletContextHandler")
        .buildChecked()
      val attachHandlerMethod = DynMethods.builder("attachHandler")
        .impl("org.apache.spark.ui.SparkUI", sparkServletContextHandlerClz)
        .buildChecked(ui)

      if (SPARK_ENGINE_RUNTIME_VERSION >= "4.0") {
        attachHandlerMethod.invoke {
          val createRedirectHandlerMethod = DynMethods.builder("createRedirectHandler")
            .impl(
              JettyUtils.getClass,
              classOf[String],
              classOf[String],
              classOf[jakarta.servlet.http.HttpServletRequest => Unit],
              classOf[String],
              classOf[Set[String]])
            .buildChecked(JettyUtils)

          val killHandler =
            (_: jakarta.servlet.http.HttpServletRequest) => handleKill()
          val gracefulKillHandler =
            (_: jakarta.servlet.http.HttpServletRequest) => handleGracefulKill()

          createRedirectHandlerMethod
            .invoke("/kyuubi/stop", "/kyuubi", killHandler, "", Set("GET", "POST"))
          createRedirectHandlerMethod
            .invoke("/kyuubi/gracefulstop", "/kyuubi", gracefulKillHandler, "", Set("GET", "POST"))
        }
      } else {
        val createRedirectHandlerMethod = DynMethods.builder("createRedirectHandler")
          .impl(
            JettyUtils.getClass,
            classOf[String],
            classOf[String],
            classOf[javax.servlet.http.HttpServletRequest => Unit],
            classOf[String],
            classOf[Set[String]])
          .buildChecked(JettyUtils)

        attachHandlerMethod.invoke {
          val killHandler =
            (_: javax.servlet.http.HttpServletRequest) => handleKill()
          val gracefulKillHandler =
            (_: javax.servlet.http.HttpServletRequest) => handleGracefulKill()

          createRedirectHandlerMethod
            .invoke("/kyuubi/stop", "/kyuubi", killHandler, "", Set("GET", "POST"))
          createRedirectHandlerMethod
            .invoke("/kyuubi/gracefulstop", "/kyuubi", gracefulKillHandler, "", Set("GET", "POST"))
        }
      }
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

  def handleKill(): Unit = {
    if (killEnabled && engine.isDefined && engine.get.getServiceState != ServiceState.STOPPED) {
      engine.get.stop()
    }
  }

  def handleGracefulKill(): Unit = {
    if (killEnabled && engine.isDefined && engine.get.getServiceState != ServiceState.STOPPED) {
      engine.get.gracefulStop()
    }
  }
}
