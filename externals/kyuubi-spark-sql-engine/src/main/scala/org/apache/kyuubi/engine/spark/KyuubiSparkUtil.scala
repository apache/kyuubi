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

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object KyuubiSparkUtil {

  def globalSparkContext: SparkContext = SparkSession.active.sparkContext

  def engineId: String =
    globalSparkContext.applicationAttemptId.getOrElse(globalSparkContext.applicationId)

  lazy val diagnostics: String = {
    val sc = globalSparkContext
    val webUrl = sc.getConf.getOption(
      "spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES")
      .orElse(sc.uiWebUrl).getOrElse("")
    // scalastyle:off line.size.limit
    s"""
       |           Spark application name: ${sc.appName}
       |                 application ID: ${engineId}
       |                 application web UI: $webUrl
       |                 master: ${sc.master}
       |                 deploy mode: ${sc.deployMode}
       |                 version: ${sc.version}
       |           Start time: ${LocalDateTime.ofInstant(Instant.ofEpochMilli(sc.startTime), ZoneId.systemDefault)}
       |           User: ${sc.sparkUser}""".stripMargin
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
}
