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

import java.util.Locale

import scala.collection.mutable

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.{EngineType, KyuubiApplicationManager}
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

    (batchKyuubiConf.getEngineConf(EngineType.SPARK_SQL) ++
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

  /**
   * Batch job submission relies on tracking a dedicated Spark driver pod on Kubernetes
   * (tagged with `kyuubi-unique-tag`) to report status, fetch logs, and support kill.
   * When `spark.submit.deployMode` is not `cluster` (either explicitly set to `client`,
   * or left unset and thus defaulting to Spark's own `client` default), Spark runs the
   * driver as a subprocess of the Kyuubi server itself instead of creating a separate
   * pod. In that case Kyuubi's tracking mechanism can never find the expected pod, and
   * users see confusing failures (e.g. "No pod was found named ...") that give no hint
   * about the actual root cause, while the batch driver also loses resource isolation
   * from the Kyuubi server process.
   *
   * Fail fast with a clear, actionable error instead of silently attempting an
   * unsupported submission mode.
   */
  override def validateConf(): Unit = {
    super.validateConf()
    val isK8sMaster = clusterManager().exists(_.toLowerCase(Locale.ROOT).startsWith("k8s"))
    if (isK8sMaster && !isClusterMode()) {
      throw new KyuubiException(
        s"Batch job submission on Kubernetes requires " +
          s"$DEPLOY_MODE_KEY=cluster, but got " +
          s"$MASTER_KEY=${clusterManager().getOrElse("")}, " +
          s"$DEPLOY_MODE_KEY=${deployMode().getOrElse("<unset, defaults to client>")}. " +
          "In client mode, Kyuubi cannot create or track a dedicated driver pod for " +
          "this batch, so status monitoring, log retrieval and kill operations would " +
          "not work correctly, and the driver would run inside the Kyuubi server's " +
          "own process without resource isolation.")
    }
  }
}
