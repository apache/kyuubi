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

package org.apache.kyuubi.engine.spark

import java.io.{BufferedReader, InputStreamReader}
import java.time.{Instant, LocalDateTime, ZoneId}

import scala.annotation.meta.getter
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.kvstore.KVIndex

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_INITIALIZE_SQL_TYPE, InitSqlType}
import org.apache.kyuubi.engine.SemanticVersion
import org.apache.kyuubi.engine.spark.SparkSQLEngine.kyuubiConf

object KyuubiSparkUtil extends Logging {

  type KVIndexParam = KVIndex @getter

  final val SPARK_SCHEDULER_POOL_KEY = "spark.scheduler.pool"
  final val SPARK_SQL_EXECUTION_ID_KEY = "spark.sql.execution.id"

  def globalSparkContext: SparkContext = SparkSession.active.sparkContext

  def initializeSparkSession(spark: SparkSession, initializationSQLs: Seq[String]): Unit = {
    initializationSQLs.foreach { sql =>
      spark.sparkContext.setJobGroup(
        "initialization sql queries",
        sql,
        interruptOnCancel = true)
      debug(s"Execute initialization sql: $sql")

      try {
        spark.sql(sql).isEmpty
      } finally {
        spark.sparkContext.clearJobGroup()
      }
    }
  }

  def getInitializeSql(sparkSession: SparkSession, config: Seq[String]): Seq[String] = {

    def getSqlTextFromFile(path: Option[String]): Seq[String] = {
      path.map(filePath => {
        var reader: BufferedReader = null
        try {
          info(s"load init sql from file: ${filePath}")
          val engineInitSqlFilePath = new Path(filePath)
          val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
          val fs: FileSystem = engineInitSqlFilePath.getFileSystem(hadoopConf)
          if (fs.exists(engineInitSqlFilePath)) {
            val sqlInputStream: FSDataInputStream = fs.open(engineInitSqlFilePath)
            reader = new BufferedReader(new InputStreamReader(sqlInputStream))
            // Filter out the sql using '--' comment
            val sqlText = ListBuffer[String]()
            Stream.continually(reader.readLine())
              .takeWhile(null != _).filter(item => !item.startsWith("--") && item.length > 0)
              .map(item => {
                if (item.endsWith(";")) {
                  item.substring(0, item.length - 1)
                } else {
                  item
                }
              }).foreach(item => sqlText.append(item))
            sqlText.toSeq
          } else {
            Nil
          }
        } catch {
          case NonFatal(e) =>
            warn(s"Failed to initialize engine initialize sql file: ${e.getMessage}")
            Nil
        } finally {
          if (null != reader) {
            reader.close()
          }
        }
      }).getOrElse(Nil)
    }

    if (kyuubiConf.get(ENGINE_INITIALIZE_SQL_TYPE) == InitSqlType.SQL.toString) {
      config
    } else {
      getSqlTextFromFile(config.headOption)
    }
  }

  def engineId: String = globalSparkContext.applicationId

  def deployMode: String = {
    if (globalSparkContext.getConf.getBoolean("spark.kubernetes.submitInDriver", false)) {
      "cluster"
    } else {
      globalSparkContext.deployMode
    }
  }

  lazy val diagnostics: String = {
    val sc = globalSparkContext
    val webUrl = sc.getConf.getOption(
      "spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES")
      .orElse(sc.uiWebUrl).getOrElse("")
    // scalastyle:off line.size.limit
    // format: off
    s"""
       |           Spark application name: ${sc.appName}
       |                 application ID: $engineId
       |                 application web UI: $webUrl
       |                 master: ${sc.master}
       |                 deploy mode: $deployMode
       |                 version: ${sc.version}
       |           Start time: ${LocalDateTime.ofInstant(Instant.ofEpochMilli(sc.startTime), ZoneId.systemDefault)}
       |           User: ${sc.sparkUser}""".stripMargin
    // format: on
    // scalastyle:on line.size.limit
  }

  /**
   * @return true if Hive classes can be loaded, otherwise false.
   */
  private[spark] def hiveClassesArePresent: Boolean = {
    try {
      Class.forName("org.apache.spark.sql.hive.HiveSessionStateBuilder")
      Class.forName("org.apache.hadoop.hive.conf.HiveConf")
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }

  lazy val sparkMajorMinorVersion: (Int, Int) = {
    val runtimeSparkVer = org.apache.spark.SPARK_VERSION
    val runtimeVersion = SemanticVersion(runtimeSparkVer)
    (runtimeVersion.majorVersion, runtimeVersion.minorVersion)
  }
}
