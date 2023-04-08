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

import java.io.File
import java.net.URL
import java.nio.file.Paths
import java.time.Instant
import java.util.Collections
import java.util.concurrent.CountDownLatch

import scala.collection.JavaConverters._

import org.apache.flink.configuration.{Configuration, DeploymentOptions, GlobalConfiguration}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.client.gateway.DefaultContextUtils
import org.apache.flink.table.gateway.service.context.DefaultContext

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.Utils.{addShutdownHook, currentUser, FLINK_ENGINE_SHUTDOWN_PRIORITY}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.flink.FlinkSQLEngine.{countDownLatch, currentEngine}
import org.apache.kyuubi.reflection.DynMethods
import org.apache.kyuubi.service.Serverable
import org.apache.kyuubi.util.SignalRegister

case class FlinkSQLEngine(engineContext: DefaultContext) extends Serverable("FlinkSQLEngine") {

  override val backendService = new FlinkSQLBackendService(engineContext)
  override val frontendServices = Seq(new FlinkTBinaryFrontendService(this))

  override def initialize(conf: KyuubiConf): Unit = super.initialize(conf)

  override protected def stopServer(): Unit = {
    countDownLatch.countDown()
  }

  override def start(): Unit = {
    super.start()
    backendService.sessionManager.startTerminatingChecker { () =>
      assert(currentEngine.isDefined)
      currentEngine.get.stop()
    }
  }
}

object FlinkSQLEngine extends Logging {

  val kyuubiConf: KyuubiConf = KyuubiConf()
  var currentEngine: Option[FlinkSQLEngine] = None

  private val user = currentUser

  private val countDownLatch = new CountDownLatch(1)

  def main(args: Array[String]): Unit = {
    SignalRegister.registerLogger(logger)

    info(s"Flink SQL engine classpath: ${System.getProperty("java.class.path")}")

    FlinkEngineUtils.checkFlinkVersion()

    try {
      kyuubiConf.loadFileDefaults()
      Utils.fromCommandLineArgs(args, kyuubiConf)
      val flinkConfDir = sys.env.getOrElse(
        "FLINK_CONF_DIR", {
          val flinkHome = sys.env.getOrElse(
            "FLINK_HOME", {
              // detect the FLINK_HOME by flink-core*.jar location if unset
              val jarLoc =
                classOf[GlobalConfiguration].getProtectionDomain.getCodeSource.getLocation
              new File(jarLoc.toURI).getParentFile.getParent
            })
          Paths.get(flinkHome, "conf").toString
        })
      val flinkConf = GlobalConfiguration.loadConfiguration(flinkConfDir)
      val flinkConfFromArgs =
        kyuubiConf.getAll.filterKeys(_.startsWith("flink."))
          .map { case (k, v) => (k.stripPrefix("flink."), v) }
      flinkConf.addAll(Configuration.fromMap(flinkConfFromArgs.asJava))

      val executionTarget = flinkConf.getString(DeploymentOptions.TARGET)
      // set cluster name for per-job and application mode
      executionTarget match {
        case "yarn-per-job" | "yarn-application" =>
          if (!flinkConf.containsKey("yarn.application.name")) {
            val appName = s"kyuubi_${user}_flink_${Instant.now}"
            flinkConf.setString("yarn.application.name", appName)
          }
          if (flinkConf.containsKey("high-availability.cluster-id")) {
            flinkConf.setString(
              "yarn.application.id",
              flinkConf.toMap.get("high-availability.cluster-id"))
          }
        case "kubernetes-application" =>
          if (!flinkConf.containsKey("kubernetes.cluster-id")) {
            val appName = s"kyuubi-${user}-flink-${Instant.now}"
            flinkConf.setString("kubernetes.cluster-id", appName)
          }
        case other =>
          debug(s"Skip generating app name for execution target $other")
      }

      val cliOptions = FlinkEngineUtils.parseCliOptions(args)
      val jars: java.util.List[URL] = Option(cliOptions.getJars).getOrElse(Collections.emptyList())
      val libDirs: java.util.List[URL] = Option(cliOptions.getLibraryDirs)
        .getOrElse(Collections.emptyList())
      val dependencies = DynMethods.builder("discoverDependencies")
        .hiddenImpl(
          classOf[DefaultContextUtils],
          classOf[java.util.List[URL]],
          classOf[java.util.List[URL]])
        .buildStatic().invoke[java.util.List[URL]](jars, libDirs)
      val engineContext = DefaultContext.load(flinkConf, dependencies, true, false)

      kyuubiConf.setIfMissing(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

      startEngine(engineContext)
      info("Flink engine started")

      if ("yarn-application".equalsIgnoreCase(executionTarget)) {
        bootstrapFlinkApplicationExecutor(flinkConf)
      }

      // blocking main thread
      countDownLatch.await()
    } catch {
      case t: Throwable if currentEngine.isDefined =>
        currentEngine.foreach { engine =>
          error(t)
          engine.stop()
        }
      case t: Throwable =>
        error("Failed to create FlinkSQL Engine", t)
    }
  }

  def startEngine(engineContext: DefaultContext): Unit = {
    currentEngine = Some(new FlinkSQLEngine(engineContext))
    currentEngine.foreach { engine =>
      engine.initialize(kyuubiConf)
      engine.start()
      addShutdownHook(() => engine.stop(), FLINK_ENGINE_SHUTDOWN_PRIORITY + 1)
    }
  }

  private def bootstrapFlinkApplicationExecutor(flinkConf: Configuration) = {
    // trigger an execution to initiate EmbeddedExecutor
    info("Running initial Flink SQL in application mode.")
    val tableEnv = TableEnvironment.create(flinkConf)
    val res = tableEnv.executeSql("select 'kyuubi'")
    res.await()
    info("Initial Flink SQL finished.")
  }
}
