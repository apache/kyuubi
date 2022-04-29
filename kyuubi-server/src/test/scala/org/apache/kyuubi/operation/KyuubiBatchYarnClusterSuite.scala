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

package org.apache.kyuubi.operation

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.WithKyuubiServerOnYarn
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.operation.OperationState.ERROR
import org.apache.kyuubi.server.api.v1.BatchRequest
import org.apache.kyuubi.session.{KyuubiBatchSessionImpl, KyuubiSessionManager}

class KyuubiBatchYarnClusterSuite extends WithKyuubiServerOnYarn {

  override protected val conf: KyuubiConf = {
    new KyuubiConf().set(s"$KYUUBI_BATCH_CONF_PREFIX.spark.spark.master", "yarn")
      .set(BATCH_CONF_IGNORE_LIST, Seq("spark.master"))
  }

  private def sessionManager(): KyuubiSessionManager =
    server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]

  test("open batch session") {
    val sparkProcessBuilder = new SparkProcessBuilder("kyuubi", conf)

    val batchRequest = BatchRequest(
      "spark",
      sparkProcessBuilder.mainResource.get,
      sparkProcessBuilder.mainClass,
      "spark-batch-submission",
      Map(
        "spark.master" -> "local",
        s"spark.${ENGINE_SPARK_MAX_LIFETIME.key}" -> "5000",
        s"spark.${ENGINE_CHECK_INTERVAL.key}" -> "1000"),
      Seq.empty[String])

    val sessionHandle = sessionManager().openBatchSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1,
      "kyuubi",
      "passwd",
      "localhost",
      batchRequest.conf,
      batchRequest)

    assert(sessionHandle.identifier.secretId === KyuubiSessionManager.STATIC_BATCH_SECRET_UUID)
    val session = sessionManager.getSession(sessionHandle).asInstanceOf[KyuubiBatchSessionImpl]
    val batchJobSubmissionOp = session.batchJobSubmissionOp

    eventually(timeout(3.minutes), interval(50.milliseconds)) {
      val state = batchJobSubmissionOp.currentApplicationState
      assert(state.nonEmpty)
      assert(state.exists(_("id").startsWith("application_")))
    }

    val killResponse = yarnOperation.killApplicationByTag(sessionHandle.identifier.toString)
    assert(killResponse._1)
    assert(killResponse._2 startsWith "Succeeded to terminate:")

    val appInfo = yarnOperation.getApplicationInfoByTag(sessionHandle.identifier.toString)

    assert(appInfo("state") === "KILLED")
    assert(batchJobSubmissionOp.getStatus.state === ERROR)

    val resultColumns = batchJobSubmissionOp.getNextRowSet(FetchOrientation.FETCH_NEXT, 10)
      .getColumns.asScala

    val keys = resultColumns.head.getStringVal.getValues.asScala
    val values = resultColumns.apply(1).getStringVal.getValues.asScala
    val rows = keys.zip(values).toMap
    val appId = rows("id")
    val appName = rows("name")
    val appState = rows("state")
    val appUrl = rows("url")
    val appError = rows("error")

    val state2 = batchJobSubmissionOp.currentApplicationState.get
    assert(appId === state2("id"))
    assert(appName === state2("name"))
    assert(appState === state2("state"))
    assert(appUrl === state2("url"))
    assert(appError === state2("error"))
    sessionManager.closeSession(sessionHandle)
  }
}
