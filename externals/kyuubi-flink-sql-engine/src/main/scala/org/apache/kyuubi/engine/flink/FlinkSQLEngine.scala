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
import java.nio.file.Paths
import java.time.Duration
import java.util.concurrent.CountDownLatch

import scala.collection.JavaConverters._

import org.apache.flink.configuration.{Configuration, DeploymentOptions, GlobalConfiguration, PipelineOptions}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.gateway.api.config.SqlGatewayServiceConfigOptions
import org.apache.flink.table.gateway.service.context.DefaultContext

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.Utils.{addShutdownHook, currentUser, FLINK_ENGINE_SHUTDOWN_PRIORITY}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_FLINK_INITIALIZE_SQL
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_ENGINE_NAME, KYUUBI_SESSION_USER_KEY}
import org.apache.kyuubi.engine.flink.FlinkSQLEngine.{countDownLatch, currentEngine}
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
          .toMap
      flinkConf.addAll(Configuration.fromMap(flinkConfFromArgs.asJava))

      val executionTarget = flinkConf.getString(DeploymentOptions.TARGET)
      setDeploymentConf(executionTarget, flinkConf)

      kyuubiConf.setIfMissing(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

      val engineContext = FlinkEngineUtils.getDefaultContext(args, flinkConf, flinkConfDir)
      startEngine(engineContext)
      info("Flink engine started")

      bootstrap(executionTarget)

      // blocking main thread
      countDownLatch.await()
    } catch {
      case t: Throwable if currentEngine.isDefined =>
        error("Fatal error occurs, thus stopping the engines", t)
        currentEngine.foreach { engine =>
          engine.stop()
        }
      case t: Throwable =>
        error("Failed to create FlinkSQL Engine", t)
    }
  }

  def startEngine(engineContext: DefaultContext): Unit = {
    debug(s"Starting Flink SQL engine with default configuration: ${engineContext.getFlinkConfig}")
    currentEngine = Some(new FlinkSQLEngine(engineContext))
    currentEngine.foreach { engine =>
      engine.initialize(kyuubiConf)
      engine.start()
      addShutdownHook(() => engine.stop(), FLINK_ENGINE_SHUTDOWN_PRIORITY + 1)
    }
  }

  private def bootstrap(executionTarget: String) = {
    val flinkConf = new Configuration()
    val tableEnv = TableEnvironment.create(flinkConf)

    if ("yarn-application".equalsIgnoreCase(executionTarget)) {
      // trigger an execution to initiate EmbeddedExecutor with the default flink conf
      flinkConf.set(PipelineOptions.NAME, "kyuubi-bootstrap-sql")
      debug(s"Running bootstrap Flink SQL in application mode with flink conf: $flinkConf.")
      tableEnv.executeSql("select 'kyuubi'").await()
    }

    kyuubiConf.get(ENGINE_FLINK_INITIALIZE_SQL).foreach { stmt =>
      tableEnv.executeSql(stmt).await()
    }

    info("Bootstrap SQL finished.")
  }

  private def setDeploymentConf(executionTarget: String, flinkConf: Configuration): Unit = {
    // forward kyuubi engine variables to flink configuration
    kyuubiConf.getOption("flink.app.name")
      .foreach(flinkConf.setString(KYUUBI_ENGINE_NAME, _))

    kyuubiConf.getOption(KYUUBI_SESSION_USER_KEY)
      .foreach(flinkConf.setString(KYUUBI_SESSION_USER_KEY, _))

    // force disable Flink's session timeout
    flinkConf.set(
      SqlGatewayServiceConfigOptions.SQL_GATEWAY_SESSION_IDLE_TIMEOUT,
      Duration.ofMillis(0))

    executionTarget match {
      case "yarn-per-job" | "yarn-application" =>
        if (flinkConf.containsKey("high-availability.cluster-id")) {
          flinkConf.setString(
            "yarn.application.id",
            flinkConf.toMap.get("high-availability.cluster-id"))
        }
      case other =>
        debug(s"Skip setting deployment conf for execution target $other")
    }
  }
}
