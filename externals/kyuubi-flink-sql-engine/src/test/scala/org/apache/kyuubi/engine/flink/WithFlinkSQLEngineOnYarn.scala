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
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiFunSuite, SCALA_COMPILE_VERSION, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_FLINK_APPLICATION_JARS, KYUUBI_HOME_ENV_VAR_NAME}
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ADDRESSES
import org.apache.kyuubi.util.JavaUtils
import org.apache.kyuubi.util.command.CommandLineUtils._
import org.apache.kyuubi.zookeeper.EmbeddedZookeeper
import org.apache.kyuubi.zookeeper.ZookeeperConf.{ZK_CLIENT_PORT, ZK_CLIENT_PORT_ADDRESS}

trait WithFlinkSQLEngineOnYarn extends KyuubiFunSuite with WithFlinkTestResources {

  protected def engineRefId: String

  protected val conf: KyuubiConf = new KyuubiConf(false)

  private var hdfsCluster: MiniDFSCluster = _

  private var yarnCluster: MiniYARNCluster = _

  private var zkServer: EmbeddedZookeeper = _

  def withKyuubiConf: Map[String, String] = testExtraConf

  private val yarnConf: YarnConfiguration = {
    val yarnConfig = new YarnConfiguration()

    // configurations copied from org.apache.flink.yarn.YarnTestBase
    yarnConfig.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 32)
    yarnConfig.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 4096)

    yarnConfig.setBoolean(YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME, true)
    yarnConfig.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2)
    yarnConfig.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, 2)
    yarnConfig.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 4)
    yarnConfig.setInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 3600)
    yarnConfig.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, false)
    // memory is overwritten in the MiniYARNCluster.
    // so we have to change the number of cores for testing.
    yarnConfig.setInt(YarnConfiguration.NM_VCORES, 666)
    yarnConfig.setFloat(YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE, 99.0f)
    yarnConfig.setInt(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, 1000)
    yarnConfig.setInt(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, 5000)

    // capacity-scheduler.xml is missing in hadoop-client-minicluster so this is a workaround
    yarnConfig.set("yarn.scheduler.capacity.root.queues", "default,four_cores_queue")

    yarnConfig.setInt("yarn.scheduler.capacity.root.default.capacity", 100)
    yarnConfig.setFloat("yarn.scheduler.capacity.root.default.user-limit-factor", 1)
    yarnConfig.setInt("yarn.scheduler.capacity.root.default.maximum-capacity", 100)
    yarnConfig.set("yarn.scheduler.capacity.root.default.state", "RUNNING")
    yarnConfig.set("yarn.scheduler.capacity.root.default.acl_submit_applications", "*")
    yarnConfig.set("yarn.scheduler.capacity.root.default.acl_administer_queue", "*")

    yarnConfig.setInt("yarn.scheduler.capacity.root.four_cores_queue.maximum-capacity", 100)
    yarnConfig.setInt("yarn.scheduler.capacity.root.four_cores_queue.maximum-applications", 10)
    yarnConfig.setInt("yarn.scheduler.capacity.root.four_cores_queue.maximum-allocation-vcores", 4)
    yarnConfig.setFloat("yarn.scheduler.capacity.root.four_cores_queue.user-limit-factor", 1)
    yarnConfig.set("yarn.scheduler.capacity.root.four_cores_queue.acl_submit_applications", "*")
    yarnConfig.set("yarn.scheduler.capacity.root.four_cores_queue.acl_administer_queue", "*")

    yarnConfig.setInt("yarn.scheduler.capacity.node-locality-delay", -1)
    // Set bind host to localhost to avoid java.net.BindException
    yarnConfig.set(YarnConfiguration.RM_BIND_HOST, "localhost")
    yarnConfig.set(YarnConfiguration.NM_BIND_HOST, "localhost")

    yarnConfig
  }

  override def beforeAll(): Unit = {
    zkServer = new EmbeddedZookeeper()
    conf.set(ZK_CLIENT_PORT, 0).set(ZK_CLIENT_PORT_ADDRESS, "localhost")
    zkServer.initialize(conf)
    zkServer.start()
    conf.set(HA_ADDRESSES, zkServer.getConnectString)

    val hdfsConf = new Configuration()
    // before HADOOP-18206 (3.4.0), HDFS MetricsLogger strongly depends on
    // commons-logging, we should disable it explicitly, otherwise, it throws
    // ClassNotFound: org.apache.commons.logging.impl.Log4JLogger
    hdfsConf.set("dfs.namenode.metrics.logger.period.seconds", "0")
    hdfsConf.set("dfs.datanode.metrics.logger.period.seconds", "0")

    hdfsCluster = new MiniDFSCluster.Builder(hdfsConf)
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
    yarnConf.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH, hadoopClasspath)

    yarnCluster = new MiniYARNCluster("flink-engine-cluster", 1, 1, 1)
    yarnCluster.init(yarnConf)
    yarnCluster.start()

    val hadoopConfDir = Utils.createTempDir().toFile
    val writer = new FileWriter(new File(hadoopConfDir, "core-site.xml"))
    yarnCluster.getConfig.writeXml(writer)
    writer.close()

    val envs = scala.collection.mutable.Map[String, String]()
    val kyuubiExternals = JavaUtils.getCodeSourceLocation(getClass)
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

    conf.set(ENGINE_FLINK_APPLICATION_JARS, udfJar.getAbsolutePath)
    val flinkExtraJars = extraFlinkJars(envs("FLINK_HOME"))
    val command = new ArrayBuffer[String]()

    command += s"${envs("FLINK_HOME")}${File.separator}bin/flink"
    command += "run-application"
    command += "-t"
    command += "yarn-application"
    command += s"-Dyarn.ship-files=${flinkExtraJars.mkString(";")}"
    command += s"-Dyarn.application.name=kyuubi_user_flink_paul"
    command += s"-Dyarn.tags=KYUUBI,$engineRefId"
    command += "-Djobmanager.memory.process.size=1g"
    command += "-Dtaskmanager.memory.process.size=1g"
    command += "-Dcontainerized.master.env.FLINK_CONF_DIR=."
    command += "-Dcontainerized.taskmanager.env.FLINK_CONF_DIR=."
    command += s"-Dcontainerized.master.env.HADOOP_CONF_DIR=${envs("HADOOP_CONF_DIR")}"
    command += s"-Dcontainerized.taskmanager.env.HADOOP_CONF_DIR=${envs("HADOOP_CONF_DIR")}"
    command += "-Dexecution.target=yarn-application"
    command += "-c"
    command += "org.apache.kyuubi.engine.flink.FlinkSQLEngine"
    command += s"${mainResource(envs).get}"

    for ((k, v) <- withKyuubiConf) {
      conf.set(k, v)
    }

    command ++= confKeyValues(conf.getAll)

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
    val flinkExtraJars = new ListBuffer[String]
    val flinkSQLJars = Paths.get(flinkHome)
      .resolve("opt")
      .toFile
      .listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = {
          name.toLowerCase.startsWith("flink-sql-client") ||
          name.toLowerCase.startsWith("flink-sql-gateway")
        }
      }).map(f => f.getAbsolutePath).sorted
    flinkExtraJars ++= flinkSQLJars

    val userJars = conf.get(ENGINE_FLINK_APPLICATION_JARS)
    userJars.foreach(jars => flinkExtraJars ++= jars.split(","))
    flinkExtraJars.toArray
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
      env.get(KYUUBI_HOME_ENV_VAR_NAME).toSeq
        .flatMap { p =>
          Seq(
            Paths.get(p, "externals", "engines", shortName, jarName),
            Paths.get(p, "externals", module, "target", jarName))
        }
        .find(Files.exists(_)).map(_.toAbsolutePath.toFile.getCanonicalPath)
    }.orElse {
      // 3. get the main resource from dev environment
      val cwd = JavaUtils.getCodeSourceLocation(getClass).split("externals")
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
    if (zkServer != null) {
      zkServer.stop()
      zkServer = null
    }
  }
}
