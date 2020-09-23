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

import java.time.Instant

import org.apache.spark.sql.SparkSession

object KyuubiSparkUtil {

  def diagnostics(spark: SparkSession): String = {
    s"""
       |           Spark application name: ${spark.sparkContext.appName}
       |                 application ID:  ${spark.sparkContext.applicationId}
       |                 application web UI: ${spark.sparkContext.uiWebUrl.getOrElse("")}
       |                 master: ${spark.sparkContext.master}
       |                 deploy mode: ${spark.sparkContext.deployMode}
       |                 version: ${spark.sparkContext.version}
       |           Start time: ${Instant.ofEpochMilli(spark.sparkContext.startTime)}
       |           User: ${spark.sparkContext.sparkUser}
       |""".stripMargin
  }
}
