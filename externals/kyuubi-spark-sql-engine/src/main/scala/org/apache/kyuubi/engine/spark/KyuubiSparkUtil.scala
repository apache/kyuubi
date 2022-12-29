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

import java.time.{Instant, LocalDateTime, ZoneId}

import scala.annotation.meta.getter

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.kvstore.KVIndex

import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.SemanticVersion

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

  def engineId: String = globalSparkContext.applicationId
  def engineName: String = globalSparkContext.appName
  def engineUrl: String = globalSparkContext.getConf.getOption(
    "spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES")
    .orElse(globalSparkContext.uiWebUrl).getOrElse("")
  def deployMode: String = {
    if (globalSparkContext.getConf.getBoolean("spark.kubernetes.submitInDriver", false)) {
      "cluster"
    } else {
      globalSparkContext.deployMode
    }
  }

  lazy val diagnostics: String = {
    val sc = globalSparkContext
    // scalastyle:off line.size.limit
    // format: off
    s"""
       |           Spark application name: $engineName
       |                 application ID: $engineId
       |                 application web UI: $engineUrl
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
