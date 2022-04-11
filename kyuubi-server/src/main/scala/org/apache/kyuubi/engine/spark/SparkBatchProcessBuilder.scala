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

import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.server.api.v1.BatchRequest
import org.apache.kyuubi.util.KyuubiHadoopUtils

class SparkBatchProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    batchRequest: BatchRequest,
    override val extraEngineLog: Option[OperationLog] = None)
  extends SparkProcessBuilder(proxyUser, conf, extraEngineLog) {
  import SparkProcessBuilder._

  private val batchJobTag = batchRequest.conf.get(TAG_KEY).map(_ + ",").getOrElse("") +
    "kyuubi-spark-" + UUID.randomUUID()

  override def mainClass: String = batchRequest.className

  override def mainResource: Option[String] = Option(batchRequest.resource)

  override protected def commands: Array[String] = {
    val buffer = new ArrayBuffer[String]()
    buffer += executable
    buffer += CLASS
    buffer += mainClass

    val allConf = batchRequest.conf ++ Map(TAG_KEY -> batchJobTag)

    allConf.foreach { case (k, v) =>
      buffer += CONF
      buffer += s"$k=$v"
    }

    buffer += PROXY_USER
    buffer += proxyUser

    mainResource.foreach { r => buffer += r }

    batchRequest.args.asScala.foreach { arg => buffer += arg }

    buffer.toArray
  }

  override protected def module: String = "kyuubi-spark-batch-submit"

  private[kyuubi] def getApplicationIdAndUrl(): Option[(String, String)] = {
    batchRequest.conf.get("spark.master") match {
      case Some("yarn") =>
        val yarnClient = getYarnClient
        val hadoopConf = KyuubiHadoopUtils.newHadoopConf(conf, engineEnv = true)
        yarnClient.init(hadoopConf)
        yarnClient.start()
        try {
          val apps = yarnClient.getApplications(null, null, Set(batchJobTag).asJava)
          apps.asScala.headOption.map { applicationReport =>
            applicationReport.getApplicationId.toString -> applicationReport.getTrackingUrl
          }
        } finally {
          yarnClient.stop()
        }

      case _ => None // TODO: Support other resource manager
    }
  }
}
