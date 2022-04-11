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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.server.api.v1.BatchRequest

class SparkBatchProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    batchRequest: BatchRequest,
    override val extraEngineLog: Option[OperationLog] = None)
  extends SparkProcessBuilder(proxyUser, conf, extraEngineLog) {
  import SparkProcessBuilder._

  private var appIdAndTrackingUrl: Option[(String, String)] = None

  override def mainClass: String = batchRequest.className

  override def mainResource: Option[String] = Option(batchRequest.resource)

  override protected def commands: Array[String] = {
    val buffer = new ArrayBuffer[String]()
    buffer += executable
    buffer += CLASS
    buffer += mainClass

    batchRequest.conf.foreach { case (k, v) =>
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

  val YARN_APP_TRACKING_REGEX =
    ".*tracking URL: (http[:/a-zA-Z0-9._-]*)(application_\\d+_\\d+).*".r("urlPrefix", "appId")

  def getAppIdAndTrackingUrl(): Option[(String, String)] = appIdAndTrackingUrl

  override protected def captureLogLine(line: String): Unit = {
    if (appIdAndTrackingUrl.isEmpty) {
      line match {
        case YARN_APP_TRACKING_REGEX(urlPrefix, appId) =>
          appIdAndTrackingUrl = Some((appId, urlPrefix + appId))
        case _ => // TODO: Support other resource manager applicationId tracking
      }
    }
  }
}
