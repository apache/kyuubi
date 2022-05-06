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

import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}

import scala.util.control.NonFatal

import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.Logging
import org.apache.kyuubi.server.api.ApiRequestContext
import org.apache.kyuubi.server.api.v1.BatchesResource.REST_BATCH_PROTOCOL
import org.apache.kyuubi.server.http.authentication.AuthenticationFilter
import org.apache.kyuubi.service.authentication.KyuubiAuthenticationFactory
import org.apache.kyuubi.session.{KyuubiBatchSessionImpl, KyuubiSessionManager}

@Tag(name = "Batch")
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class BatchesResource extends ApiRequestContext with Logging {

  private def sessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[Batch]))),
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
    val session = sessionManager.getSession(sessionHandle).asInstanceOf[KyuubiBatchSessionImpl]
    buildBatch(session)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[Batch]))),
    description = "get the batch info via batch id")
  @GET
  @Path("{batchId}")
  def batchInfo(@PathParam("batchId") batchId: String): Batch = {
    try {
      val sessionHandle = sessionManager.getBatchSessionHandle(batchId, REST_BATCH_PROTOCOL)
      val session = sessionManager.getSession(sessionHandle).asInstanceOf[KyuubiBatchSessionImpl]
      buildBatch(session)
    } catch {
      case NonFatal(e) =>
        error(s"Invalid batchId: $batchId", e)
        throw new NotFoundException(s"Invalid batchId: $batchId")
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[GetBatchListResponse]))),
    description = "returns the active batch sessions")
  @GET
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def getBatchInfoList(
      @QueryParam("batchType") batchType: String,
      @QueryParam("from") from: Int,
      @QueryParam("size") size: Int): GetBatchListResponse = {
    val sessions = sessionManager.getBatchSessionList(batchType, from, size)
    val batches = sessions.map { session =>
      val batchSession = session.asInstanceOf[KyuubiBatchSessionImpl]
      buildBatch(batchSession)
    }
    GetBatchListResponse(from, batches.size, batches)
  }

  private def buildBatch(session: KyuubiBatchSessionImpl): Batch = {
    val batchOp = session.batchJobSubmissionOp
    Batch(
      batchOp.batchId,
      batchOp.batchType,
      batchOp.currentApplicationState.getOrElse(Map.empty),
      fe.connectionUrl,
      batchOp.getStatus.state.toString)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON)),
    description = "close a batch session")
  @DELETE
  @Path("{batchId}")
  def closeBatchSession(
      @PathParam("batchId") batchId: String,
      @QueryParam("killApp") killApp: Boolean,
      @QueryParam("hive.server2.proxy.user") hs2ProxyUser: String): Response = {
    var session: KyuubiBatchSessionImpl = null
    try {
      val sessionHandle = sessionManager.getBatchSessionHandle(batchId, REST_BATCH_PROTOCOL)
      session = sessionManager.getSession(sessionHandle).asInstanceOf[KyuubiBatchSessionImpl]
    } catch {
      case NonFatal(e) =>
        error(s"Invalid batchId: $batchId", e)
        throw new NotFoundException(s"Invalid batchId: $batchId")
    }

    val sessionConf = Option(hs2ProxyUser).filter(_.nonEmpty).map(proxyUser =>
      Map(KyuubiAuthenticationFactory.HS2_PROXY_USER -> proxyUser)).getOrElse(Map())

    var userName: String = null
    try {
      userName = fe.getUserName(sessionConf)
    } catch {
      case t: Throwable =>
        throw new NotAllowedException(t.getMessage)
    }

    if (!session.user.equals(userName)) {
      throw new NotAllowedException(
        s"$userName is not allowed to close the session belong to ${session.user}")
    }

    if (killApp) {
      val killResponse = session.batchJobSubmissionOp.killBatchApplication()
      sessionManager.closeSession(session.handle)
      Response.ok().entity(killResponse).build()
    } else {
      sessionManager.closeSession(session.handle)
      Response.ok().build()
    }
  }
}

object BatchesResource {
  val REST_BATCH_PROTOCOL = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11
}
