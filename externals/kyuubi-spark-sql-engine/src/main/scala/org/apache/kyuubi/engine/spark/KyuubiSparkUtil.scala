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

import java.net.URI
import java.time.{Instant, LocalDateTime, ZoneId}

import scala.annotation.meta.getter

import org.apache.spark.{SPARK_VERSION, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.kyuubi.SparkDatasetHelper
import org.apache.spark.util.kvstore.KVIndex

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.ConfigEntry
import org.apache.kyuubi.util.SemanticVersion
import org.apache.kyuubi.util.reflect.DynMethods

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
        SparkDatasetHelper.commandResultOptimized(spark.sql(sql)).isEmpty
      } finally {
        spark.sparkContext.clearJobGroup()
      }
    }
  }

  def engineId: String = globalSparkContext.applicationId
  def engineName: String = globalSparkContext.appName
  def engineUrl: String = {
    val sparkConf = globalSparkContext.getConf
    sparkConf
      // scalastyle:off line.size.limit
      // format: off
      // for Spark 3.5 or before
      .getOption("spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES")
      // for Spark 4.0 or later, see SPARK-48238
      .orElse(sparkConf.getOption("spark.org.apache.spark.deploy.yarn.AmIpFilter.param.PROXY_URI_BASES"))
      // format: on
      // scalastyle:on line.size.limit
      .orElse(globalSparkContext.uiWebUrl)
      .getOrElse("")
  }
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

  // Given that we are on the Spark SQL engine side, the [[org.apache.spark.SPARK_VERSION]] can be
  // represented as the runtime version of the Spark SQL engine.
  lazy val SPARK_ENGINE_RUNTIME_VERSION: SemanticVersion = SemanticVersion(SPARK_VERSION)

  /**
   * Get session level config value
   * @param configEntry configEntry
   * @param spark sparkSession
   * @tparam T any type
   * @return session level config value, if spark not set this config,
   *         default return kyuubi's config
   */
  def getSessionConf[T](configEntry: ConfigEntry[T], spark: SparkSession): T = {
    spark.conf.getOption(configEntry.key).map(configEntry.valueConverter).getOrElse {
      SparkSQLEngine.kyuubiConf.get(configEntry)
    }
  }

  // SPARK-47118 (4.0.0) upgrades Jersey from 2 to 3 which also changes javax.ws.rs to
  // jakarta.ws.rs, this is an equivalent implementation using reflection of the following
  // plain invocation:
  //   {javax|jakarta}.ws.rs.core.UriBuilder.fromUri(uri).fragment(fragment).build()
  def buildURI(uri: URI, fragment: String): URI = {
    if (SPARK_ENGINE_RUNTIME_VERSION >= "4.0") {
      var uriBuilder = DynMethods.builder("fromUri")
        .impl("jakarta.ws.rs.core.UriBuilder", classOf[URI])
        .buildStatic()
        .invoke[AnyRef](uri)

      uriBuilder = DynMethods.builder("fragment")
        .impl("jakarta.ws.rs.core.UriBuilder", classOf[String])
        .build(uriBuilder)
        .invoke[AnyRef](fragment)

      DynMethods.builder("build")
        .impl("jakarta.ws.rs.core.UriBuilder", classOf[Array[AnyRef]])
        .build(uriBuilder)
        .invoke[URI](Array.empty[AnyRef])
    } else {
      var uriBuilder = DynMethods.builder("fromUri")
        .impl("javax.ws.rs.core.UriBuilder", classOf[URI])
        .buildStatic()
        .invoke[AnyRef](uri)

      uriBuilder = DynMethods.builder("fragment")
        .impl("javax.ws.rs.core.UriBuilder", classOf[String])
        .build(uriBuilder)
        .invoke[AnyRef](fragment)

      DynMethods.builder("build")
        .impl("javax.ws.rs.core.UriBuilder", classOf[Array[AnyRef]])
        .build(uriBuilder)
        .invoke[URI](Array.empty[AnyRef])
    }
  }
}
