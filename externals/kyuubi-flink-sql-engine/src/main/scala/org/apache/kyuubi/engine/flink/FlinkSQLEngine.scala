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
import java.net.{InetAddress, URL}
import java.time.Instant
import java.util.concurrent.CountDownLatch

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

import org.apache.flink.client.cli.{CliFrontend, CustomCommandLine, DefaultCLI, GenericCLI}
import org.apache.flink.configuration.{Configuration, DeploymentOptions, GlobalConfiguration}
import org.apache.flink.table.client.SqlClientException
import org.apache.flink.table.client.gateway.context.DefaultContext
import org.apache.flink.util.JarUtils

import org.apache.kyuubi.{KyuubiSQLException, Logging, Utils}
import org.apache.kyuubi.Utils.{addShutdownHook, currentUser, FLINK_ENGINE_SHUTDOWN_PRIORITY}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_EVENT_JSON_LOG_PATH, ENGINE_EVENT_LOGGERS}
import org.apache.kyuubi.engine.flink.FlinkSQLEngine.{countDownLatch, currentEngine}
import org.apache.kyuubi.engine.flink.event.FlinkEngineEvent
import org.apache.kyuubi.engine.flink.event.handler.FlinkJsonLoggingEventHandler
import org.apache.kyuubi.events.{EventBus, EventLoggerType, KyuubiEvent}
import org.apache.kyuubi.service.Serverable
import org.apache.kyuubi.util.{KyuubiHadoopUtils, SignalRegister}

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
    .set(ENGINE_EVENT_LOGGERS.key, "JSON")
  var currentEngine: Option[FlinkSQLEngine] = None

  private val user = currentUser

  private val countDownLatch = new CountDownLatch(1)

  def main(args: Array[String]): Unit = {
    SignalRegister.registerLogger(logger)

    FlinkEngineUtils.checkFlinkVersion()

    try {
      val flinkConfDir = CliFrontend.getConfigurationDirectoryFromEnv
      val flinkConf = GlobalConfiguration.loadConfiguration(flinkConfDir)
      val flinkConfFromSys =
        Utils.getSystemProperties.filterKeys(_.startsWith("flink."))
          .map { case (k, v) => (k.stripPrefix("flink."), v) }
      flinkConf.addAll(Configuration.fromMap(flinkConfFromSys.asJava))

      val executionTarget = flinkConf.getString(DeploymentOptions.TARGET)
      // set cluster name for per-job and application mode
      executionTarget match {
        case "yarn-per-job" | "yarn-application" =>
          if (!flinkConf.containsKey("yarn.application.name")) {
            val appName = s"kyuubi_${user}_flink_${Instant.now}"
            flinkConf.setString("yarn.application.name", appName)
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
      val jars = if (cliOptions.getJars != null) cliOptions.getJars.asScala else List.empty
      val libDirs =
        if (cliOptions.getLibraryDirs != null) cliOptions.getLibraryDirs.asScala else List.empty
      val dependencies = discoverDependencies(jars, libDirs)
      val engineContext = new DefaultContext(
        dependencies.asJava,
        flinkConf,
        List[CustomCommandLine](new GenericCLI(flinkConf, flinkConfDir), new DefaultCLI).asJava)

      kyuubiConf.setIfMissing(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

      startEngine(engineContext)
      info("started engine...")

      // blocking main thread
      countDownLatch.await()
    } catch {
      case t: Throwable if currentEngine.isDefined =>
        currentEngine.foreach { engine =>
          error(t)
          engine.stop()
          val event = FlinkEngineEvent(engine)
            .copy(endTime = System.currentTimeMillis(), diagnostic = t.getMessage)
          EventBus.post(event)
        }
      case t: Throwable =>
        error("Create FlinkSQL Engine Failed", t)
    }
  }

  def startEngine(engineContext: DefaultContext): Unit = {
    try {
      initLoggerEventHandler(kyuubiConf)
    } catch {
      case NonFatal(e) =>
        warn(s"Failed to initialize Logger EventHandler: ${e.getMessage}", e)
    }
    currentEngine = Some(new FlinkSQLEngine(engineContext))
    currentEngine.foreach { engine =>
      engine.initialize(kyuubiConf)
      engine.start()
      val event = FlinkEngineEvent(engine)
      EventBus.post(event)
      addShutdownHook(
        () => {
          engine.stop()
          val event = FlinkEngineEvent(engine)
            .copy(endTime = System.currentTimeMillis())
          EventBus.post(event)
        },
        FLINK_ENGINE_SHUTDOWN_PRIORITY + 1)
    }
  }

  private def initLoggerEventHandler(conf: KyuubiConf): Unit = {
    val hadoopConf = KyuubiHadoopUtils.newHadoopConf(conf)
    conf.get(ENGINE_EVENT_LOGGERS).map(EventLoggerType.withName).foreach {
      case EventLoggerType.JSON =>
        val hostName = InetAddress.getLocalHost.getCanonicalHostName
        val handler = FlinkJsonLoggingEventHandler(
          s"Flink-$hostName",
          ENGINE_EVENT_JSON_LOG_PATH,
          hadoopConf,
          conf)
        EventBus.register[KyuubiEvent](handler)
      case logger =>
        throw new IllegalArgumentException(s"Unrecognized event logger: $logger")

    }
  }

  private def discoverDependencies(
      jars: Seq[URL],
      libraries: Seq[URL]): List[URL] = {
    try {
      var dependencies: ListBuffer[URL] = ListBuffer()
      // find jar files
      jars.foreach { url =>
        JarUtils.checkJarFile(url)
        dependencies = dependencies += url
      }
      // find jar files in library directories
      libraries.foreach { libUrl =>
        val dir: File = new File(libUrl.toURI)
        if (!dir.isDirectory) throw new SqlClientException("Directory expected: " + dir)
        else if (!dir.canRead) throw new SqlClientException("Directory cannot be read: " + dir)
        val files: Array[File] = dir.listFiles
        if (files == null) throw new SqlClientException("Directory cannot be read: " + dir)
        files.foreach { f =>
          // only consider jars
          if (f.isFile && f.getAbsolutePath.toLowerCase.endsWith(".jar")) {
            val url: URL = f.toURI.toURL
            JarUtils.checkJarFile(url)
            dependencies = dependencies += url
          }
        }
      }
      dependencies.toList
    } catch {
      case e: Exception =>
        throw KyuubiSQLException(s"Could not load all required JAR files.", e)
    }
  }
}
