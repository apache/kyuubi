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

package org.apache.kyuubi.server.api.v1

import javax.ws.rs.{Consumes, POST, Produces}
import javax.ws.rs.core.MediaType

import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.Logging
import org.apache.kyuubi.server.api.ApiRequestContext
import org.apache.kyuubi.server.api.v1.BatchesResource.REST_BATCH_PROTOCOL
import org.apache.kyuubi.server.http.authentication.AuthenticationFilter
import org.apache.kyuubi.session.{KyuubiBatchSessionImpl, KyuubiSessionManager}

@Tag(name = "Batch")
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class BatchesResource extends ApiRequestContext with Logging {

  private def sessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON)),
    description = "create and open a batch session")
  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def openBatchSession(request: BatchRequest): Batch = {
    val userName = fe.getUserName(request.conf)
    val ipAddress = AuthenticationFilter.getUserIpAddress
    val sessionHandle = sessionManager.openBatchSession(
      REST_BATCH_PROTOCOL,
      userName,
      "anonymous",
      ipAddress,
      Option(request.conf).getOrElse(Map()),
      request)
    val batchOp = sessionManager.getSession(sessionHandle).asInstanceOf[
      KyuubiBatchSessionImpl].batchJobSubmissionOp
    Batch(
      batchOp.batchId,
      batchOp.batchType,
      batchOp.currentApplicationState.getOrElse(Map.empty),
      fe.connectionUrl,
      batchOp.getStatus.state.toString)
  }
}

object BatchesResource {
  val REST_BATCH_PROTOCOL = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11
}
