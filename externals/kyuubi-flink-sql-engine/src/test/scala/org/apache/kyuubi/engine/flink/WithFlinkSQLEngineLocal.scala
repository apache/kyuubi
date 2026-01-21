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

import java.io.{File, FilenameFilter}
import java.lang.ProcessBuilder.Redirect
import java.net.URI
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.runtime.minicluster.{MiniCluster, MiniClusterConfiguration}

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiException, KyuubiFunSuite, SCALA_COMPILE_VERSION}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ADDRESSES
import org.apache.kyuubi.util.JavaUtils
import org.apache.kyuubi.util.command.CommandLineUtils._
import org.apache.kyuubi.zookeeper.EmbeddedZookeeper
import org.apache.kyuubi.zookeeper.ZookeeperConf.{ZK_CLIENT_PORT, ZK_CLIENT_PORT_ADDRESS}

trait WithFlinkSQLEngineLocal extends KyuubiFunSuite with WithFlinkTestResources {

  protected val flinkConfig = new Configuration()

  protected var miniCluster: MiniCluster = _

  protected var engineProcess: Process = _

  private var zkServer: EmbeddedZookeeper = _

  protected val conf: KyuubiConf = new KyuubiConf(false)

  protected def engineRefId: String

  def withKyuubiConf: Map[String, String]

  protected var connectionUrl: String = _

  override def beforeAll(): Unit = {
    withKyuubiConf.foreach { case (k, v) =>
      if (k.startsWith("flink.")) {
        flinkConfig.setString(k.stripPrefix("flink."), v)
      }
    }
    withKyuubiConf.foreach { case (k, v) =>
      conf.set(k, v)
    }

    zkServer = new EmbeddedZookeeper()
    conf.set(ZK_CLIENT_PORT, 0).set(ZK_CLIENT_PORT_ADDRESS, "localhost")
    zkServer.initialize(conf)
    zkServer.start()
    conf.set(HA_ADDRESSES, zkServer.getConnectString)

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
    envs("JAVA_HOME") = System.getProperty("java.home")
    envs("JAVA_EXEC") = Paths.get(envs("JAVA_HOME"), "bin", "java").toString

    startMiniCluster()
    startFlinkEngine(envs.toMap)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (engineProcess != null) {
      engineProcess.destroy()
      engineProcess = null
    }
    if (miniCluster != null) {
      miniCluster.close()
      miniCluster = null
    }
    if (zkServer != null) {
      zkServer.stop()
      zkServer = null
    }
  }

  def startFlinkEngine(envs: Map[String, String]): Unit = {
    val flinkHome = envs("FLINK_HOME")
    val processBuilder: ProcessBuilder = new ProcessBuilder
    processBuilder.environment().putAll(envs.asJava)

    conf.set(ENGINE_FLINK_EXTRA_CLASSPATH, udfJar.getAbsolutePath)
    val command = new mutable.ListBuffer[String]()

    command += envs("JAVA_EXEC")

    val memory = conf.get(ENGINE_FLINK_MEMORY)
    command += s"-Xmx$memory"
    val javaOptions = conf.get(ENGINE_FLINK_JAVA_OPTIONS).filter(StringUtils.isNotBlank(_))
    if (javaOptions.isDefined) {
      command += javaOptions.get
    }

    val classpathEntries = new mutable.LinkedHashSet[String]
    // flink engine runtime jar
    mainResource(envs).foreach(classpathEntries.add)
    // flink sql jars
    Paths.get(flinkHome)
      .resolve("opt")
      .toFile
      .listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = {
          name.toLowerCase.startsWith("flink-sql-client") ||
          name.toLowerCase.startsWith("flink-sql-gateway")
        }
      }).foreach(jar => classpathEntries.add(jar.getAbsolutePath))

    // jars from flink lib
    classpathEntries.add(s"$flinkHome${File.separator}lib${File.separator}*")

    // classpath contains flink configurations, default to flink.home/conf
    classpathEntries.add(envs.getOrElse("FLINK_CONF_DIR", ""))
    // classpath contains hadoop configurations
    val cp = System.getProperty("java.class.path")
    // exclude kyuubi flink engine jar that has SPI for EmbeddedExecutorFactory
    // which can't be initialized on the client side
    val hadoopJars = cp.split(":").filter(s => !s.contains("flink"))
    hadoopJars.foreach(classpathEntries.add)
    val extraCp = conf.get(ENGINE_FLINK_EXTRA_CLASSPATH)
    extraCp.foreach(classpathEntries.add)
    if (hadoopJars.isEmpty && extraCp.isEmpty) {
      mainResource(envs).foreach { path =>
        val devHadoopJars = Paths.get(path).getParent
          .resolve(s"scala-$SCALA_COMPILE_VERSION")
          .resolve("jars")
        if (!Files.exists(devHadoopJars)) {
          throw new KyuubiException(s"The path $devHadoopJars does not exists. " +
            s"Please set FLINK_HADOOP_CLASSPATH or ${ENGINE_FLINK_EXTRA_CLASSPATH.key}" +
            s" for configuring location of hadoop client jars, etc.")
        }
        classpathEntries.add(s"$devHadoopJars${File.separator}*")
      }
    }
    command ++= genClasspathOption(classpathEntries)

    command += "org.apache.kyuubi.engine.flink.FlinkSQLEngine"

    command ++= confKeyValues(conf.getAll)

    processBuilder.command(command.toList.asJava)
    processBuilder.redirectOutput(Redirect.INHERIT)
    processBuilder.redirectError(Redirect.INHERIT)

    info(s"staring flink local engine...")
    engineProcess = processBuilder.start()
  }

  private def startMiniCluster(): Unit = {
    val cfg = new MiniClusterConfiguration.Builder()
      .setConfiguration(flinkConfig)
      .setNumSlotsPerTaskManager(1)
      .setNumTaskManagers(2)
      .build
    miniCluster = new MiniCluster(cfg)
    miniCluster.start()
    flinkConfig.setString(RestOptions.ADDRESS, miniCluster.getRestAddress.get().getHost)
    flinkConfig.setInteger(RestOptions.PORT, miniCluster.getRestAddress.get().getPort)
  }

  protected def getJdbcUrl: String = s"jdbc:hive2://$connectionUrl/;"

  def mainResource(env: Map[String, String]): Option[String] = {
    val module = "kyuubi-flink-sql-engine"
    val shortName = "flink"
    // 1. get the main resource jar for user specified config first
    val jarName = s"${module}_$SCALA_COMPILE_VERSION-$KYUUBI_VERSION.jar"
    conf.getOption(s"kyuubi.session.engine.$shortName.main.resource").filter {
      userSpecified =>
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
}
