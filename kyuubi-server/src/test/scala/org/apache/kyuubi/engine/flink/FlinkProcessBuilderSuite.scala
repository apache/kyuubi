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

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

import org.apache.kyuubi.{FLINK_COMPILE_VERSION, KyuubiException, KyuubiFunSuite, SCALA_COMPILE_VERSION}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_FLINK_EXTRA_CLASSPATH, ENGINE_FLINK_JAVA_OPTIONS, ENGINE_FLINK_MEMORY}

class FlinkProcessBuilderSuite extends KyuubiFunSuite {
  private def conf = KyuubiConf().set("kyuubi.on", "off")
    .set(ENGINE_FLINK_MEMORY, "512m")
    .set(
      ENGINE_FLINK_JAVA_OPTIONS,
      "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005")
    .set("yarn.tags", "KYUUBI")

  private def envDefault: ListMap[String, String] = ListMap(
    "JAVA_HOME" -> s"${File.separator}jdk")
  private def envWithoutHadoopCLASSPATH: ListMap[String, String] = envDefault +
    ("HADOOP_CONF_DIR" -> s"${File.separator}hadoop${File.separator}conf") +
    ("YARN_CONF_DIR" -> s"${File.separator}yarn${File.separator}conf") +
    ("HBASE_CONF_DIR" -> s"${File.separator}hbase${File.separator}conf")
  private def envWithAllHadoop: ListMap[String, String] = envWithoutHadoopCLASSPATH +
    ("FLINK_HADOOP_CLASSPATH" -> s"${File.separator}hadoop")
  private def confStr: String = {
    conf.getAll.map { case (k, v) => s"\\\n\t--conf $k=$v" }.mkString(" ")
  }
  private def compareActualAndExpected(builder: FlinkProcessBuilder) = {
    val actualCommands = builder.toString
    val classpathStr: String = constructClasspathStr(builder)
    val expectedCommands = s"$javaPath -Xmx512m " +
      s"-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 " +
      s"-cp $classpathStr $mainClassStr \\\n\t--conf kyuubi.session.user=vinoyang " +
      s"$confStr"
    info(s"\n\n actualCommands $actualCommands")
    info(s"\n\n expectedCommands $expectedCommands")
    assert(actualCommands == expectedCommands)
  }

  private def constructClasspathStr(builder: FlinkProcessBuilder) = {
    val classpathEntries = new java.util.LinkedHashSet[String]
    builder.mainResource.foreach(classpathEntries.add)

    val flinkHomeField = classOf[FlinkProcessBuilder].getDeclaredField("flinkHome")
    flinkHomeField.setAccessible(true)
    val flinkHome = flinkHomeField.get(builder).asInstanceOf[String]

    val flinkSqlClientJarPath = s"$flinkHome$flinkSqlClientJarPathSuffix"
    val flinkLibPath = s"$flinkHome$flinkLibPathSuffix"
    val flinkConfPath = s"$flinkHome$flinkConfPathSuffix"
    classpathEntries.add(flinkSqlClientJarPath)
    classpathEntries.add(flinkLibPath)
    classpathEntries.add(flinkConfPath)
    val envMethod = classOf[FlinkProcessBuilder].getDeclaredMethod("env")
    envMethod.setAccessible(true)
    val envMap = envMethod.invoke(builder).asInstanceOf[Map[String, String]]
    envMap.foreach { case (k, v) =>
      if (!k.equals("JAVA_HOME")) {
        classpathEntries.add(v)
      }
    }
    val extraCp = conf.get(ENGINE_FLINK_EXTRA_CLASSPATH)
    extraCp.foreach(classpathEntries.add)
    val classpathStr = classpathEntries.asScala.mkString(File.pathSeparator)
    classpathStr
  }

  private val javaPath = s"${envDefault("JAVA_HOME")}${File.separator}bin${File.separator}java"
  private val flinkSqlClientJarPathSuffix = s"${File.separator}opt${File.separator}" +
    s"flink-sql-client_$SCALA_COMPILE_VERSION-$FLINK_COMPILE_VERSION.jar"
  private val flinkLibPathSuffix = s"${File.separator}lib${File.separator}*"
  private val flinkConfPathSuffix = s"${File.separator}conf"
  private val mainClassStr = "org.apache.kyuubi.engine.flink.FlinkSQLEngine"

  test("all hadoop related environment variables are configured") {
    val builder = new FlinkProcessBuilder("vinoyang", conf) {
      override protected def env: Map[String, String] = envWithAllHadoop

    }
    compareActualAndExpected(builder)
  }

  test("all hadoop related environment variables are configured except FLINK_HADOOP_CLASSPATH") {
    val builder = new FlinkProcessBuilder("vinoyang", conf) {
      override def env: Map[String, String] = envWithoutHadoopCLASSPATH
    }
    assertThrows[KyuubiException](builder.toString)
  }

  test("only FLINK_HADOOP_CLASSPATH environment variables are configured") {
    val builder = new FlinkProcessBuilder("vinoyang", conf) {
      override def env: Map[String, String] = envDefault +
        ("FLINK_HADOOP_CLASSPATH" -> s"${File.separator}hadoop")
    }
    compareActualAndExpected(builder)
  }
}
