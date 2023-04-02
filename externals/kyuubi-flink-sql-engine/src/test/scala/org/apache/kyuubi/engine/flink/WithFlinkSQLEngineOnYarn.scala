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

import java.io.{File, FilenameFilter, FileWriter}
import java.lang.ProcessBuilder.Redirect
import java.net.URI
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiFunSuite, SCALA_COMPILE_VERSION, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.KYUUBI_HOME

trait WithFlinkSQLEngineOnYarn extends KyuubiFunSuite {

  protected def engineRefId: String

  protected val conf: KyuubiConf = new KyuubiConf(false)

  private var hdfsCluster: MiniDFSCluster = _

  private var yarnCluster: MiniYARNCluster = _

  def withKyuubiConf: Map[String, String]

  private val yarnConf: YarnConfiguration = {
    val yarnConfig = new YarnConfiguration()
    yarnConfig.set("yarn.scheduler.minimum-allocation-mb", "256")
    yarnConfig.set("yarn.resourcemanager.webapp.address", "localhost:8088")
    // Disable the disk utilization check to avoid the test hanging when people's disks are
    // getting full.
    yarnConfig.set(
      "yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage",
      "100.0")

    // capacity-scheduler.xml is missing in hadoop-client-minicluster so this is a workaround
    yarnConfig.set("yarn.scheduler.capacity.root.queues", "default,two_cores_queue")

    yarnConfig.setInt("yarn.scheduler.capacity.root.default.capacity", 100)
    yarnConfig.setFloat("yarn.scheduler.capacity.root.default.user-limit-factor", 1)
    yarnConfig.setInt("yarn.scheduler.capacity.root.default.maximum-capacity", 100)
    yarnConfig.set("yarn.scheduler.capacity.root.default.state", "RUNNING")
    yarnConfig.set("yarn.scheduler.capacity.root.default.acl_submit_applications", "*")
    yarnConfig.set("yarn.scheduler.capacity.root.default.acl_administer_queue", "*")

    yarnConfig.setInt("yarn.scheduler.capacity.root.two_cores_queue.maximum-capacity", 100)
    yarnConfig.setInt("yarn.scheduler.capacity.root.two_cores_queue.maximum-applications", 2)
    yarnConfig.setInt("yarn.scheduler.capacity.root.two_cores_queue.maximum-allocation-vcores", 2)
    yarnConfig.setFloat("yarn.scheduler.capacity.root.two_cores_queue.user-limit-factor", 1)
    yarnConfig.set("yarn.scheduler.capacity.root.two_cores_queue.acl_submit_applications", "*")
    yarnConfig.set("yarn.scheduler.capacity.root.two_cores_queue.acl_administer_queue", "*")

    yarnConfig.setInt("yarn.scheduler.capacity.node-locality-delay", -1)
    // Set bind host to localhost to avoid java.net.BindException
    yarnConfig.set("yarn.resourcemanager.bind-host", "localhost")

    yarnConfig
  }

  override def beforeAll(): Unit = {
    hdfsCluster = new MiniDFSCluster.Builder(new Configuration)
      .numDataNodes(1)
      .checkDataNodeAddrConfig(true)
      .checkDataNodeHostConfig(true)
      .build()

    val hdfsServiceUrl = s"hdfs://localhost:${hdfsCluster.getNameNodePort}"
    yarnConf.set("fs.defaultFS", hdfsServiceUrl)
    yarnConf.addResource(hdfsCluster.getConfiguration(0))

    val cp = System.getProperty("java.class.path")
    // exclude kyuubi flink engine jar that has SPI for EmbeddedExecutorFactory
    // which can't be initialized on the client side
    val hadoopJars = cp.split(":").filter(s => !s.contains("flink") && !s.contains("log4j"))
    val hadoopClasspath = hadoopJars.mkString(":")
    yarnConf.set("yarn.application.classpath", hadoopClasspath)

    yarnCluster = new MiniYARNCluster("flink-engine-cluster", 1, 1, 1)
    yarnCluster.init(yarnConf)
    yarnCluster.start()

    val hadoopConfDir = Utils.createTempDir().toFile
    val writer = new FileWriter(new File(hadoopConfDir, "yarn-site.xml"))
    yarnCluster.getConfig.writeXml(writer)
    writer.close()

    val envs = scala.collection.mutable.Map[String, String]()
    val kyuubiExternals = Utils.getCodeSourceLocation(getClass)
      .split("externals").head
    val flinkHome = {
      val candidates = Paths.get(kyuubiExternals, "externals", "kyuubi-download", "target")
        .toFile.listFiles(f => f.getName.contains("flink"))
      if (candidates == null) None else candidates.map(_.toPath).headOption
    }
    if (flinkHome.isDefined) {
      envs("FLINK_HOME") = flinkHome.get.toString
      envs("FLINK_CONF_DIR") = Paths.get(flinkHome.get.toString, "conf").toString
    }
    envs("HADOOP_CLASSPATH") = hadoopClasspath
    envs("HADOOP_CONF_DIR") = hadoopConfDir.getAbsolutePath

    startFlinkEngine(envs.toMap)

    super.beforeAll()
  }

  private def startFlinkEngine(envs: Map[String, String]): Unit = {
    val processBuilder: ProcessBuilder = new ProcessBuilder
    processBuilder.environment().putAll(envs.asJava)

    val flinkExtraJars = extraFlinkJars(envs("FLINK_HOME"))
    val command = new ArrayBuffer[String]()

    command += s"${envs("FLINK_HOME")}${File.separator}bin/flink"
    command += "run-application"
    command += "-t"
    command += "yarn-application"
    command += s"-Dpipeline.jars=${flinkExtraJars.mkString(",")}"
    command += s"-Dyarn.ship-files=${flinkExtraJars.mkString(";")}"
    command += s"-Dyarn.tags=KYUUBI,$engineRefId"
    command += "-Djobmanager.memory.process.size=1g"
    command += "-Dtaskmanager.memory.process.size=1g"
    command += "-Dcontainerized.master.env.FLINK_CONF_DIR=."
    command += "-Dexecution.target=yarn-application"
    command += "-c"
    command += "org.apache.kyuubi.engine.flink.FlinkSQLEngine"
    command += s"${mainResource(envs).get}"

    val effectiveKyuubiConf = conf.getAll ++ withKyuubiConf
    for ((k, v) <- effectiveKyuubiConf) {
      command += "--conf"
      command += s"$k=$v"
    }

    processBuilder.command(command.toList.asJava)
    processBuilder.redirectOutput(Redirect.INHERIT)
    processBuilder.redirectError(Redirect.INHERIT)

    info(s"staring flink yarn-application cluster for engine $engineRefId..")
    val process = processBuilder.start()
    process.waitFor()
    info(s"flink yarn-application cluster for engine $engineRefId has started")
  }

  def extraFlinkJars(flinkHome: String): Array[String] = {
    // locate flink sql jars
    val flinkExtraJars = Paths.get(flinkHome)
      .resolve("opt")
      .toFile
      .listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = {
          name.toLowerCase.startsWith("flink-sql-client") ||
          name.toLowerCase.startsWith("flink-sql-gateway")
        }
      }).map(f => f.getAbsolutePath).sorted
    flinkExtraJars
  }

  /**
   * Copied form org.apache.kyuubi.engine.ProcBuilder
   * The engine jar or other runnable jar containing the main method
   */
  def mainResource(env: Map[String, String]): Option[String] = {
    // 1. get the main resource jar for user specified config first
    val module = "kyuubi-flink-sql-engine"
    val shortName = "flink"
    val jarName = s"${module}_$SCALA_COMPILE_VERSION-$KYUUBI_VERSION.jar"
    conf.getOption(s"kyuubi.session.engine.$shortName.main.resource").filter { userSpecified =>
      // skip check exist if not local file.
      val uri = new URI(userSpecified)
      val schema = if (uri.getScheme != null) uri.getScheme else "file"
      schema match {
        case "file" => Files.exists(Paths.get(userSpecified))
        case _ => true
      }
    }.orElse {
      // 2. get the main resource jar from system build default
      env.get(KYUUBI_HOME).toSeq
        .flatMap { p =>
          Seq(
            Paths.get(p, "externals", "engines", shortName, jarName),
            Paths.get(p, "externals", module, "target", jarName))
        }
        .find(Files.exists(_)).map(_.toAbsolutePath.toFile.getCanonicalPath)
    }.orElse {
      // 3. get the main resource from dev environment
      val cwd = Utils.getCodeSourceLocation(getClass).split("externals")
      assert(cwd.length > 1)
      Option(Paths.get(cwd.head, "externals", module, "target", jarName))
        .map(_.toAbsolutePath.toFile.getCanonicalPath)
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (yarnCluster != null) {
      yarnCluster.stop()
      yarnCluster = null
    }
    if (hdfsCluster != null) {
      hdfsCluster.shutdown()
      hdfsCluster = null
    }
  }
}
