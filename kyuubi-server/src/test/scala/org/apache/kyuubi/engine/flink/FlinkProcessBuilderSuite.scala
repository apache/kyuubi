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

import org.apache.kyuubi.{FLINK_COMPILE_VERSION, KyuubiFunSuite, KyuubiSQLException, SCALA_COMPILE_VERSION}
import org.apache.kyuubi.config.KyuubiConf

class FlinkProcessBuilderSuite extends KyuubiFunSuite {
  private def conf = KyuubiConf().set("kyuubi.on", "off")
  private def envDefault: ListMap[String, String] = ListMap(
    "JAVA_HOME" -> s"${File.separator}jdk1.8.0_181")
  private def envWithoutHadoopCLASSPATH: ListMap[String, String] = envDefault +
    ("HADOOP_CONF_DIR" -> s"${File.separator}hadoop${File.separator}conf") +
    ("YARN_CONF_DIR" -> s"${File.separator}yarn${File.separator}conf") +
    ("HBASE_CONF_DIR" -> s"${File.separator}hbase${File.separator}conf")
  private def envWithAllHadoop: ListMap[String, String] = envWithoutHadoopCLASSPATH +
    ("HADOOP_CLASSPATH" -> s"${File.separator}hadoop")
  private def confStr: String = {
    conf.getAll.filter { case (k, _) =>
      k.startsWith("kyuubi.") || k.startsWith("flink.") ||
        k.startsWith("hadoop.") || k.startsWith("yarn.")
    }.map { case (k, v) => s"-D$k=$v" }.mkString(" ")
  }
  private def compareActualAndExpected(builder: FlinkProcessBuilder) = {
    val actualCommands = builder.toString
    val classpathStr: String = constructClasspathStr(builder)
    val expectedCommands = s"bash -c source ${builder.FLINK_HOME}" +
      s"${File.separator}bin${File.separator}config.sh && $javaPath " +
      s"-Dkyuubi.session.user=vinoyang $confStr" +
      s" -cp $classpathStr $mainClassStr"
    info(s"\n\n actualCommands $actualCommands")
    info(s"\n\n expectedCommands $expectedCommands")
    assert(actualCommands.equals(expectedCommands))
  }

  private def constructClasspathStr(builder: FlinkProcessBuilder) = {
    val classpathEntries = new java.util.LinkedHashSet[String]
    builder.mainResource.foreach(classpathEntries.add)
    val flinkSqlClientJarPath = s"${builder.FLINK_HOME}$flinkSqlClientJarPathSuffix"
    val flinkLibPath = s"${builder.FLINK_HOME}$flinkLibPathSuffix"
    val flinkConfPath = s"${builder.FLINK_HOME}$flinkConfPathSuffix"
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

  test("all hadoop related environment variables are configured except HADOOP_CLASSPATH") {
    val builder = new FlinkProcessBuilder("vinoyang", conf) {
      override def env: Map[String, String] = envWithoutHadoopCLASSPATH
    }
    assertThrows[KyuubiSQLException](builder.toString)
  }

  test("only HADOOP_CLASSPATH environment variables are configured") {
    val builder = new FlinkProcessBuilder("vinoyang", conf) {
      override def env: Map[String, String] = envDefault +
        ("HADOOP_CLASSPATH" -> s"${File.separator}hadoop")
    }
    compareActualAndExpected(builder)
  }
}
