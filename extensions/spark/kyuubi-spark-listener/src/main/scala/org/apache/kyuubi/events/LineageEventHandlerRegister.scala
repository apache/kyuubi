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

package org.apache.kyuubi.events

import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.SparkListenerSQLConf

object LineageEventHandlerRegister {

  @volatile var isRegister: Boolean = false

  def register(spark: SparkSession): Unit = {
    if (!isRegister) {
      val logPath =
        spark.conf.getOption(SparkListenerSQLConf.LINEAGE_EVENT_JSON_LOG_PATH.key).getOrElse(
          SparkListenerSQLConf.LINEAGE_EVENT_JSON_LOG_PATH.defaultValStr)
      val handler = LineageJsonLoggingEventHandler(
        spark.sparkContext.applicationAttemptId
          .map(id => s"${spark.sparkContext.applicationId}_$id")
          .getOrElse(spark.sparkContext.applicationId),
        spark.sparkContext.hadoopConfiguration,
        logPath)
      EventBus.register[OperationLineageEvent](handler)
      isRegister = true
    }
  }

}
