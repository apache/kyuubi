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
package org.apache.kyuubi.engine.spark.events

import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_EVENT_JSON_LOG_PATH, ENGINE_SPARK_EVENT_LOGGERS}
import org.apache.kyuubi.engine.spark.events.handler.{SparkHistoryLoggingEventHandler, SparkJsonLoggingEventHandler}
import org.apache.kyuubi.events.{EventHandlerRegister, KyuubiEvent}
import org.apache.kyuubi.events.handler.{EventHandler, HttpLoggingEventHandler}

class SparkEventHandlerRegister(spark: SparkSession) extends EventHandlerRegister {

  override protected def createSparkEventHandler(kyuubiConf: KyuubiConf)
      : EventHandler[KyuubiEvent] = {
    new SparkHistoryLoggingEventHandler(spark.sparkContext)
  }

  override protected def createJsonEventHandler(kyuubiConf: KyuubiConf)
      : EventHandler[KyuubiEvent] = {
    SparkJsonLoggingEventHandler(
      spark.sparkContext.applicationAttemptId
        .map(id => s"${spark.sparkContext.applicationId}_$id")
        .getOrElse(spark.sparkContext.applicationId),
      ENGINE_EVENT_JSON_LOG_PATH,
      spark.sparkContext.hadoopConfiguration,
      kyuubiConf)
  }

  override protected def createHttpEventHandler(kyuubiConf: KyuubiConf)
      : EventHandler[KyuubiEvent] = {
    HttpLoggingEventHandler(ENGINE_SPARK_EVENT_LOGGERS, kyuubiConf)
  }

  override protected def getLoggers(conf: KyuubiConf): Seq[String] = {
    conf.get(ENGINE_SPARK_EVENT_LOGGERS)
  }
}
