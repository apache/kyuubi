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
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.util.matching.Regex

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_FLINK_APPLICATION_JARS, ENGINE_FLINK_EXTRA_CLASSPATH, ENGINE_FLINK_JAVA_OPTIONS, ENGINE_FLINK_MEMORY}
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_ENGINE_CREDENTIALS_KEY
import org.apache.kyuubi.engine.flink.FlinkProcessBuilder._

class FlinkProcessBuilderSuite extends KyuubiFunSuite {
  private def sessionModeConf = KyuubiConf()
    .set("flink.execution.target", "yarn-session")
    .set("kyuubi.on", "off")
    .set(ENGINE_FLINK_MEMORY, "512m")
    .set(
      ENGINE_FLINK_JAVA_OPTIONS,
      "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005")
    .set(KYUUBI_ENGINE_CREDENTIALS_KEY, "should-not-be-used")

  private def applicationModeConf = KyuubiConf()
    .set("flink.execution.target", "yarn-application")
    .set(ENGINE_FLINK_APPLICATION_JARS, tempUdfJar.toString)
    .set(APP_KEY, "kyuubi_connection_flink_paul")
    .set("kyuubi.on", "off")
    .set(KYUUBI_ENGINE_CREDENTIALS_KEY, "should-not-be-used")

  private val tempFlinkHome = Files.createTempDirectory("flink-home").toFile
  private val tempOpt =
    Files.createDirectories(Paths.get(tempFlinkHome.toPath.toString, "opt")).toFile
  Files.createFile(Paths.get(tempOpt.toPath.toString, "flink-sql-client-1.17.2.jar"))
  Files.createFile(Paths.get(tempOpt.toPath.toString, "flink-sql-gateway-1.17.2.jar"))
  private val tempUsrLib =
    Files.createDirectories(Paths.get(tempFlinkHome.toPath.toString, "usrlib")).toFile
  private val tempUdfJar =
    Files.createFile(Paths.get(tempUsrLib.toPath.toString, "test-udf.jar"))
  private val tempHiveDir =
    Files.createDirectories(Paths.get(tempFlinkHome.toPath.toString, "hive-conf")).toFile
  Files.createFile(Paths.get(tempHiveDir.toPath.toString, "hive-site.xml"))

  private def envDefault: ListMap[String, String] = ListMap(
    "JAVA_HOME" -> s"${File.separator}jdk",
    "FLINK_HOME" -> s"${tempFlinkHome.toPath}")
  private def envWithoutHadoopCLASSPATH: ListMap[String, String] = envDefault +
    ("HADOOP_CONF_DIR" -> s"${File.separator}hadoop${File.separator}conf") +
    ("YARN_CONF_DIR" -> s"${File.separator}yarn${File.separator}conf") +
    ("HBASE_CONF_DIR" -> s"${File.separator}hbase${File.separator}conf") +
    ("HIVE_CONF_DIR" -> s"$tempHiveDir")
  private def envWithAllHadoop: ListMap[String, String] = envWithoutHadoopCLASSPATH +
    (FLINK_HADOOP_CLASSPATH_KEY -> s"${File.separator}hadoop")
  private def confStr: String = {
    sessionModeConf.clone.getAll
      .map { case (k, v) => s"\\\\\\n\\t--conf $k=$v" }
      .mkString(" ")
  }

  private def matchActualAndExpectedSessionMode(builder: FlinkProcessBuilder): Unit = {
    val actualCommands = builder.toString
    val classpathStr = constructClasspathStr(builder)
    val expectedCommands =
      s"""$javaPath \\\\
         |\\t-Xmx512m \\\\
         |\\t-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 \\\\
         |\\t-cp $classpathStr $mainClassStr \\\\
         |\\t--conf kyuubi.session.user=vinoyang $confStr""".stripMargin
    val regex = new Regex(expectedCommands)
    val matcher = regex.pattern.matcher(actualCommands)
    assert(matcher.matches())
  }

  private def matchActualAndExpectedApplicationMode(builder: FlinkProcessBuilder): Unit = {
    val actualCommands = builder.toString
    // scalastyle:off line.size.limit
    val expectedCommands =
      escapePaths(
        s"""${builder.flinkExecutable} run-application \\\\
           |\\t-t yarn-application \\\\
           |\\t-Dyarn.ship-files=.*flink-sql-client.*jar;.*flink-sql-gateway.*jar;$tempUdfJar;.*hive-site.xml \\\\
           |\\t-Dyarn.application.name=kyuubi_.* \\\\
           |\\t-Dyarn.tags=KYUUBI \\\\
           |\\t-Dcontainerized.master.env.FLINK_CONF_DIR=. \\\\
           |\\t-Dcontainerized.master.env.HIVE_CONF_DIR=. \\\\
           |\\t-Dexecution.target=yarn-application \\\\
           |\\t-c org.apache.kyuubi.engine.flink.FlinkSQLEngine .*kyuubi-flink-sql-engine_.*jar""".stripMargin +
          "(?: \\\\\\n\\t--conf \\S+=\\S+)+")
    // scalastyle:on line.size.limit
    val regex = new Regex(expectedCommands)
    val matcher = regex.pattern.matcher(actualCommands)
    assert(matcher.matches())
  }

  private def escapePaths(path: String): String = {
    path.replaceAll("/", "\\/")
  }

  private def constructClasspathStr(builder: FlinkProcessBuilder) = {
    val classpathEntries = new java.util.LinkedHashSet[String]
    builder.mainResource.foreach(classpathEntries.add)

    val flinkHome = builder.flinkHome
    classpathEntries.add(s"$flinkHome$flinkSqlClientJarPathSuffixRegex")
    classpathEntries.add(s"$flinkHome$flinkSqlGatewayJarPathSuffixRegex")
    classpathEntries.add(s"$flinkHome$flinkLibPathSuffixRegex")
    classpathEntries.add(s"$flinkHome$flinkConfPathSuffix")
    val envMap = builder.env
    envMap.foreach { case (k, v) =>
      if (!k.equals("JAVA_HOME") && !k.equals("FLINK_HOME")) {
        classpathEntries.add(v)
      }
    }
    val extraCp = sessionModeConf.get(ENGINE_FLINK_EXTRA_CLASSPATH)
    extraCp.foreach(classpathEntries.add)
    val classpathStr = classpathEntries.asScala.mkString(File.pathSeparator)
    classpathStr
  }

  private val javaPath = s"${envDefault("JAVA_HOME")}${File.separator}bin${File.separator}java"
  private val flinkSqlClientJarPathSuffixRegex = s"${File.separator}opt${File.separator}" +
    s"flink-sql-client-.*.jar"
  private val flinkSqlGatewayJarPathSuffixRegex = s"${File.separator}opt${File.separator}" +
    s"flink-sql-gateway-.*.jar"
  private val flinkLibPathSuffixRegex = s"${File.separator}lib${File.separator}\\*"
  private val flinkConfPathSuffix = s"${File.separator}conf"
  private val mainClassStr = "org.apache.kyuubi.engine.flink.FlinkSQLEngine"

  test("session mode - all hadoop related environment variables are configured") {
    val builder = new FlinkProcessBuilder("vinoyang", true, sessionModeConf) {
      override def env: Map[String, String] = envWithAllHadoop
    }
    matchActualAndExpectedSessionMode(builder)
  }

  test("session mode - only FLINK_HADOOP_CLASSPATH environment variables are configured") {
    val builder = new FlinkProcessBuilder("vinoyang", true, sessionModeConf) {
      override def env: Map[String, String] = envDefault +
        (FLINK_HADOOP_CLASSPATH_KEY -> s"${File.separator}hadoop")
    }
    matchActualAndExpectedSessionMode(builder)
  }

  test("application mode - all hadoop related environment variables are configured") {
    val builder = new FlinkProcessBuilder("paullam", true, applicationModeConf) {
      override def env: Map[String, String] = envWithAllHadoop
    }
    matchActualAndExpectedApplicationMode(builder)
  }

  test("user configuration takes priority") {
    val customShipFiles = "testFile1.jar;testFile2.jar"
    val customAppName = "testAppName"
    val customYarnTags = "testTag1,testTag2"
    val builderConf = applicationModeConf
    builderConf.set("flink.yarn.ship-files", customShipFiles)
    builderConf.set("flink.yarn.application.name", customAppName)
    builderConf.set("flink.yarn.tags", customYarnTags)
    val builder = new FlinkProcessBuilder("test", true, builderConf) {
      override def env: Map[String, String] = envWithAllHadoop
    }
    val actualCommands = builder.toString
    // scalastyle:off line.size.limit
    val expectedCommands =
      escapePaths(
        s"""${builder.flinkExecutable} run-application \\\\
           |\\t-t yarn-application \\\\
           |\\t-Dyarn.ship-files=.*flink-sql-client.*jar;.*flink-sql-gateway.*jar;$tempUdfJar;.*hive-site.xml;$customShipFiles \\\\
           |\\t-Dyarn.application.name=$customAppName \\\\
           |\\t-Dyarn.tags=$customYarnTags,KYUUBI \\\\
           |\\t-Dcontainerized.master.env.FLINK_CONF_DIR=. \\\\
           |\\t-Dcontainerized.master.env.HIVE_CONF_DIR=. \\\\
           |\\t-Dexecution.target=yarn-application \\\\
           |\\t-c org.apache.kyuubi.engine.flink.FlinkSQLEngine .*kyuubi-flink-sql-engine_.*jar""".stripMargin +
          "(?: \\\\\\n\\t--conf \\S+=\\S+)+")
    // scalastyle:on line.size.limit
    val regex = new Regex(expectedCommands)
    val matcher = regex.pattern.matcher(actualCommands)
    assert(matcher.matches())
  }
}
