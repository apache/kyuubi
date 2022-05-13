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

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.server.api.v1.BatchRequest

class SparkBatchProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    batchId: String,
    batchRequest: BatchRequest,
    override val extraEngineLog: Option[OperationLog])
  extends SparkProcessBuilder(proxyUser, conf, extraEngineLog) {
  import SparkProcessBuilder._

  override def mainClass: String = batchRequest.className

  override def mainResource: Option[String] = Option(batchRequest.resource)

  override protected val commands: Array[String] = {
    val buffer = new ArrayBuffer[String]()
    buffer += executable
    Option(mainClass).foreach { cla =>
      buffer += CLASS
      buffer += cla
    }

    val batchJobTag = batchRequest.conf.get(TAG_KEY).map(_ + ",").getOrElse("") + batchId

    val allConf = batchRequest.conf ++ Map(TAG_KEY -> batchJobTag) ++ sparkAppNameConf()

    allConf.foreach { case (k, v) =>
      buffer += CONF
      buffer += s"$k=$v"
    }

    // iff the keytab is specified, PROXY_USER is not supported
    if (!useKeytab()) {
      buffer += PROXY_USER
      buffer += proxyUser
    }

    assert(mainResource.isDefined)
    buffer += mainResource.get

    batchRequest.args.foreach { arg => buffer += arg }

    buffer.toArray
  }

  private def sparkAppNameConf(): Map[String, String] = {
    Option(batchRequest.name).filterNot(_.isEmpty).map { appName =>
      Map(APP_KEY -> appName)
    }.getOrElse(Map())
  }

  override protected def module: String = "kyuubi-spark-batch-submit"

  override def clusterManager(): Option[String] = {
    batchRequest.conf.get(MASTER_KEY).orElse(defaultMaster)
  }
}
