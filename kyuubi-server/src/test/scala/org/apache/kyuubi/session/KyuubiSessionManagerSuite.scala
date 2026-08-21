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

package org.apache.kyuubi.session

import java.util.UUID

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.FRONTEND_PROTOCOLS
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.server.metadata.MetadataManager
import org.apache.kyuubi.server.metadata.api.Metadata

class KyuubiSessionManagerSuite extends KyuubiFunSuite {

  test("[KYUUBI #7244] sanitize null-valued batch metadata before submission") {
    withSessionManager { sessionManager =>
      val batchId = UUID.randomUUID().toString
      val metadata = newMetadata(
        identifier = batchId,
        kyuubiInstance = "localhost:10009",
        requestConf = Map("spark.master" -> "local", "spark.executor.memory" -> null))
        .copy(state = OperationState.INITIALIZED.toString)

      val session = sessionManager.createBatchSession(
        metadata.username,
        "anonymous",
        metadata.ipAddress,
        metadata.requestConf,
        metadata.engineType,
        Option(metadata.requestName),
        metadata.resource,
        metadata.className,
        metadata.requestArgs,
        Some(metadata),
        fromRecovery = false)

      assert(session.handle.identifier.toString === batchId)
      assert(!session.normalizedConf.contains("spark.executor.memory"))
    }
  }

  test("[KYUUBI #7244] sanitize null-valued batch metadata during recovery") {
    withSessionManager { sessionManager =>
      val validBatchId = UUID.randomUUID().toString
      val invalidBatchId = UUID.randomUUID().toString
      val kyuubiInstance = "localhost:10009"
      val recoveryMetadata = Seq(
        newMetadata(
          identifier = validBatchId,
          kyuubiInstance = kyuubiInstance,
          requestConf = Map("spark.master" -> "local")),
        newMetadata(
          identifier = invalidBatchId,
          kyuubiInstance = kyuubiInstance,
          requestConf = Map("spark.master" -> "local", "spark.executor.memory" -> null)))

      sessionManager.metadataManager = Some(new RecoveryMetadataManager(recoveryMetadata))

      val sessions = sessionManager.getBatchSessionsToRecover(kyuubiInstance)

      assert(sessions.map(_.handle.identifier.toString).toSet === Set(validBatchId, invalidBatchId))
      val recoveredInvalidSession = sessions.find(
        _.handle.identifier.toString == invalidBatchId).get
      assert(!recoveredInvalidSession.normalizedConf.contains("spark.executor.memory"))

      val recoveredMetadata = sessionManager.getBatchMetadata(invalidBatchId).get
      assert(recoveredMetadata.state === OperationState.PENDING.toString)
      assert(recoveredMetadata.engineError.isEmpty)
    }
  }

  private class RecoveryMetadataManager(recoveryMetadata: Seq[Metadata]) extends MetadataManager {
    private var updatedMetadata: Option[Metadata] = None

    override def getBatchesRecoveryMetadata(
        state: String,
        kyuubiInstance: String,
        from: Int,
        size: Int): Seq[Metadata] = {
      recoveryMetadata
        .filter(metadata => metadata.state == state && metadata.kyuubiInstance == kyuubiInstance)
        .slice(from, from + size)
    }

    override def getBatchSessionMetadata(batchId: String): Option[Metadata] = {
      updatedMetadata
        .filter(_.identifier == batchId)
        .orElse(recoveryMetadata.find(_.identifier == batchId))
    }

    override def updateMetadata(metadata: Metadata, asyncRetryOnError: Boolean): Unit = {
      updatedMetadata = Some(metadata)
    }
  }

  private def withSessionManager(f: KyuubiSessionManager => Unit): Unit = {
    val sessionManager = new KyuubiSessionManager()
    val conf = KyuubiConf()
      .set(FRONTEND_PROTOCOLS, Seq(FrontendProtocols.REST.toString))
    try {
      sessionManager.initialize(conf)
      sessionManager.start()
      f(sessionManager)
    } finally {
      sessionManager.stop()
    }
  }

  private def newMetadata(
      identifier: String,
      kyuubiInstance: String,
      requestConf: Map[String, String]): Metadata = {
    Metadata(
      identifier = identifier,
      sessionType = SessionType.BATCH,
      realUser = "kyuubi",
      username = "kyuubi",
      ipAddress = "127.0.0.1",
      kyuubiInstance = kyuubiInstance,
      state = OperationState.PENDING.toString,
      resource = "intern",
      className = "org.apache.kyuubi.SparkWC",
      requestName = "kyuubi_batch",
      requestConf = requestConf,
      requestArgs = Seq.empty,
      createTime = System.currentTimeMillis(),
      engineType = "spark",
      clusterManager = Some("local"))
  }
}
