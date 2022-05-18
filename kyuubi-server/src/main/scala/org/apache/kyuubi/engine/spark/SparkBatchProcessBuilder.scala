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

import org.apache.kyuubi.client.api.v1.dto.BatchRequest
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.log.OperationLog

class SparkBatchProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    batchId: String,
    batchRequest: BatchRequest,
    override val extraEngineLog: Option[OperationLog])
  extends SparkProcessBuilder(proxyUser, conf, extraEngineLog) {
  import SparkProcessBuilder._

  override def mainClass: String = batchRequest.getClassName

  override def mainResource: Option[String] = Option(batchRequest.getResource)

  override protected val commands: Array[String] = {
    val buffer = new ArrayBuffer[String]()
    buffer += executable
    Option(mainClass).foreach { cla =>
      buffer += CLASS
      buffer += cla
    }

    val batchJobTag = batchRequest.getConf.asScala.get(TAG_KEY).map(_ + ",").getOrElse("") + batchId

    val allConf = batchRequest.getConf.asScala ++ Map(TAG_KEY -> batchJobTag) ++ sparkAppNameConf()

    allConf.foreach { case (k, v) =>
      buffer += CONF
      buffer += s"$k=$v"
    }

    buffer += PROXY_USER
    buffer += proxyUser

    assert(mainResource.isDefined)
    buffer += mainResource.get

    batchRequest.getArgs.asScala.foreach { arg => buffer += arg }

    buffer.toArray
  }

  private def sparkAppNameConf(): Map[String, String] = {
    Option(batchRequest.getName).filterNot(_.isEmpty).map { appName =>
      Map(APP_KEY -> appName)
    }.getOrElse(Map())
  }

  override protected def module: String = "kyuubi-spark-batch-submit"

  override def clusterManager(): Option[String] = {
    batchRequest.getConf.asScala.get(MASTER_KEY).orElse(defaultMaster)
  }
}
