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

import scala.collection.mutable

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.KyuubiApplicationManager
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.util.command.CommandLineUtils._

class SparkBatchProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    batchId: String,
    batchName: String,
    override val mainResource: Option[String],
    override val mainClass: String,
    batchConf: Map[String, String],
    batchArgs: Seq[String],
    override val extraEngineLog: Option[OperationLog])
// TODO respect doAsEnabled
  extends SparkProcessBuilder(proxyUser, true, conf, batchId, extraEngineLog) {
  import SparkProcessBuilder._

  override protected[kyuubi] lazy val commands: Iterable[String] = {
    val buffer = new mutable.ListBuffer[String]()
    buffer += executable
    Option(mainClass).foreach { cla =>
      buffer += CLASS
      buffer += cla
    }

    val batchKyuubiConf = new KyuubiConf(false)
    // complete `spark.master` if absent on kubernetes
    completeMasterUrl(batchKyuubiConf)
    batchConf.foreach(entry => { batchKyuubiConf.set(entry._1, entry._2) })
    // tag batch application
    KyuubiApplicationManager.tagApplication(batchId, "spark", clusterManager(), batchKyuubiConf)

    (batchKyuubiConf.getAll ++
      sparkAppNameConf() ++
      engineLogPathConf() ++
      appendPodNameConf(batchConf) ++
      prepareK8sFileUploadPath() ++
      engineWaitCompletionConf()).map { case (k, v) =>
      buffer ++= confKeyValue(convertConfigKey(k), v)
    }

    setupKerberos(buffer)

    assert(mainResource.isDefined)
    buffer += mainResource.get

    batchArgs.foreach { arg => buffer += arg }

    buffer
  }

  private def sparkAppNameConf(): Map[String, String] = {
    Option(batchName).filterNot(_.isEmpty).map { appName =>
      Map(APP_KEY -> appName)
    }.getOrElse(Map())
  }

  override protected def module: String = "kyuubi-spark-batch-submit"

  override private[spark] def getSparkOption(key: String) = {
    batchConf.get(key).orElse(super.getSparkOption(key))
  }
}
