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

import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import javax.ws.rs._
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.client.api.v1.dto._
import org.apache.kyuubi.client.exception.KyuubiRestException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.{BatchJobSubmission, FetchOrientation, OperationState}
import org.apache.kyuubi.server.api.ApiRequestContext
import org.apache.kyuubi.server.api.v1.BatchesResource._
import org.apache.kyuubi.server.http.authentication.AuthenticationFilter
import org.apache.kyuubi.server.metadata.MetadataManager
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.service.authentication.KyuubiAuthenticationFactory
import org.apache.kyuubi.session.{KyuubiBatchSessionImpl, KyuubiSessionManager, SessionHandle}

@Tag(name = "Batch")
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class BatchesResource extends ApiRequestContext with Logging {
  private val internalRestClients = new ConcurrentHashMap[String, InternalRestClient]()
  private lazy val internalSocketTimeout =
    fe.getConf.get(KyuubiConf.BATCH_INTERNAL_REST_CLIENT_SOCKET_TIMEOUT)
  private lazy val internalConnectTimeout =
    fe.getConf.get(KyuubiConf.BATCH_INTERNAL_REST_CLIENT_CONNECT_TIMEOUT)

  private def getInternalRestClient(kyuubiInstance: String): InternalRestClient = {
    internalRestClients.computeIfAbsent(
      kyuubiInstance,
      kyuubiInstance => {
        new InternalRestClient(
          kyuubiInstance,
          internalSocketTimeout.toInt,
          internalConnectTimeout.toInt)
      })
  }

  private def sessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]

  private def buildBatch(session: KyuubiBatchSessionImpl): Batch = {
    val batchOp = session.batchJobSubmissionOp
    val batchOpStatus = batchOp.getStatus
    new Batch(
      batchOp.batchId,
      session.user,
      batchOp.batchType,
      batchOp.batchName,
      batchOp.applicationStatus.getOrElse(Map.empty).asJava,
      fe.connectionUrl,
      batchOpStatus.state.toString,
      session.createTime,
      batchOpStatus.completed)
  }

  private def buildBatch(
      metadata: Metadata,
      batchAppStatus: Option[Map[String, String]]): Batch = {
    batchAppStatus.map { appStatus =>
      var realBatchState = metadata.state

      if (BatchJobSubmission.applicationFailed(batchAppStatus)) {
        realBatchState = OperationState.ERROR.toString
      } else if (BatchJobSubmission.applicationTerminated(batchAppStatus)) {
        realBatchState = OperationState.FINISHED.toString
      }

      new Batch(
        metadata.identifier,
        metadata.username,
        metadata.engineType,
        metadata.requestName,
        appStatus.asJava,
        metadata.kyuubiInstance,
        realBatchState,
        metadata.createTime,
        metadata.endTime)
    }.getOrElse(MetadataManager.buildBatch(metadata))
  }

  private def formatSessionHandle(sessionHandleStr: String): SessionHandle = {
    try {
      SessionHandle.fromUUID(sessionHandleStr)
    } catch {
      case e: IllegalArgumentException =>
        throw new NotFoundException(s"Invalid batchId: $sessionHandleStr", e)
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[Batch]))),
    description = "create and open a batch session")
  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def openBatchSession(request: BatchRequest): Batch = {
    require(
      supportedBatchType(request.getBatchType),
      s"${request.getBatchType} is not in the supported list: $SUPPORTED_BATCH_TYPES}")
    require(request.getResource != null, "resource is a required parameter")
    require(request.getClassName != null, "classname is a required parameter")
    request.setBatchType(request.getBatchType.toUpperCase(Locale.ROOT))

    val userName = fe.getUserName(request.getConf.asScala.toMap)
    val ipAddress = AuthenticationFilter.getUserIpAddress
    val sessionHandle = sessionManager.openBatchSession(
      userName,
      "anonymous",
      ipAddress,
      request.getConf.asScala.toMap,
      request)
    buildBatch(sessionManager.getBatchSessionImpl(sessionHandle))
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
    val sessionHandle = formatSessionHandle(batchId)
    Option(sessionManager.getBatchSessionImpl(sessionHandle)).map { batchSession =>
      buildBatch(batchSession)
    }.getOrElse {
      Option(sessionManager.getBatchMetadata(batchId)).map { metadata =>
        if (OperationState.isTerminal(OperationState.withName(metadata.state))) {
          MetadataManager.buildBatch(metadata)
        } else {
          val batchAppStatus = sessionManager.applicationManager.getApplicationInfo(
            metadata.clusterManager,
            batchId)
          buildBatch(metadata, batchAppStatus)
        }
      }.getOrElse {
        error(s"Invalid batchId: $batchId")
        throw new NotFoundException(s"Invalid batchId: $batchId")
      }
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[GetBatchesResponse]))),
    description = "returns the batch sessions.")
  @GET
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def getBatchInfoList(
      @QueryParam("batchType") batchType: String,
      @QueryParam("batchState") batchState: String,
      @QueryParam("batchUser") batchUser: String,
      @QueryParam("createTime") createTime: Long,
      @QueryParam("endTime") endTime: Long,
      @QueryParam("from") from: Int,
      @QueryParam("size") size: Int): GetBatchesResponse = {
    require(
      createTime >= 0 && endTime >= 0 && (endTime == 0 || createTime <= endTime),
      "Invalid time range")
    if (batchState != null) {
      require(
        validBatchState(batchState),
        s"The valid batch state can be one of the following: ${VALID_BATCH_STATES.mkString(",")}")
    }
    val batches =
      sessionManager.getBatchesFromMetadataStore(
        batchType,
        batchUser,
        batchState,
        createTime,
        endTime,
        from,
        size)
    new GetBatchesResponse(from, batches.size, batches.asJava)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[OperationLog]))),
    description = "get the local log lines from this batch")
  @GET
  @Path("{batchId}/localLog")
  def getBatchLocalLog(
      @PathParam("batchId") batchId: String,
      @QueryParam("from") @DefaultValue("-1") from: Int,
      @QueryParam("size") size: Int): OperationLog = {
    val userName = fe.getUserName(Map.empty)
    val sessionHandle = formatSessionHandle(batchId)
    Option(sessionManager.getBatchSessionImpl(sessionHandle)).map { batchSession =>
      try {
        val submissionOp = batchSession.batchJobSubmissionOp
        val rowSet = submissionOp.getOperationLogRowSet(
          FetchOrientation.FETCH_NEXT,
          from,
          size)
        val logRowSet = rowSet.getColumns.get(0).getStringVal.getValues.asScala
        new OperationLog(logRowSet.asJava, logRowSet.size)
      } catch {
        case NonFatal(e) =>
          val errorMsg = s"Error getting operation log for batchId: $batchId"
          error(errorMsg, e)
          throw new NotFoundException(errorMsg)
      }
    }.getOrElse {
      Option(sessionManager.getBatchMetadata(batchId)).map { metadata =>
        if (fe.connectionUrl != metadata.kyuubiInstance) {
          val internalRestClient = getInternalRestClient(metadata.kyuubiInstance)
          internalRestClient.getBatchLocalLog(userName, batchId, from, size)
        } else {
          throw new NotFoundException(s"No local log found for batch: $batchId")
        }
      }.getOrElse {
        error(s"Invalid batchId: $batchId")
        throw new NotFoundException(s"Invalid batchId: $batchId")
      }
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[CloseBatchResponse]))),
    description = "close and cancel a batch session")
  @DELETE
  @Path("{batchId}")
  def closeBatchSession(
      @PathParam("batchId") batchId: String,
      @QueryParam("hive.server2.proxy.user") hs2ProxyUser: String): CloseBatchResponse = {
    val sessionHandle = formatSessionHandle(batchId)

    val sessionConf = Option(hs2ProxyUser).filter(_.nonEmpty).map(proxyUser =>
      Map(KyuubiAuthenticationFactory.HS2_PROXY_USER -> proxyUser)).getOrElse(Map())

    var userName: String = null
    try {
      userName = fe.getUserName(sessionConf)
    } catch {
      case t: Throwable =>
        throw new NotAllowedException(t.getMessage)
    }

    Option(sessionManager.getBatchSessionImpl(sessionHandle)).map { batchSession =>
      if (userName != batchSession.user) {
        throw new NotAllowedException(
          s"$userName is not allowed to close the session belong to ${batchSession.user}")
      }
      sessionManager.closeSession(batchSession.handle)
      val (success, msg) = batchSession.batchJobSubmissionOp.getKillMessage
      new CloseBatchResponse(success, msg)
    }.getOrElse {
      Option(sessionManager.getBatchMetadata(batchId)).map { metadata =>
        if (userName != metadata.username) {
          throw new NotAllowedException(
            s"$userName is not allowed to close the session belong to ${metadata.username}")
        } else if (OperationState.isTerminal(OperationState.withName(metadata.state)) ||
          metadata.kyuubiInstance == fe.connectionUrl) {
          new CloseBatchResponse(false, s"The batch[$metadata] has been terminated.")
        } else {
          info(s"Redirecting delete batch[$batchId] to ${metadata.kyuubiInstance}")
          val internalRestClient = getInternalRestClient(metadata.kyuubiInstance)
          try {
            internalRestClient.deleteBatch(userName, batchId)
          } catch {
            case e: KyuubiRestException =>
              error(s"Error redirecting delete batch[$batchId] to ${metadata.kyuubiInstance}", e)
              val appMgrKillResp = sessionManager.applicationManager.killApplication(
                metadata.clusterManager,
                batchId)
              if (appMgrKillResp._1) {
                new CloseBatchResponse(appMgrKillResp._1, appMgrKillResp._2)
              } else {
                new CloseBatchResponse(false, Utils.stringifyException(e))
              }
          }
        }
      }.getOrElse {
        error(s"Invalid batchId: $batchId")
        throw new NotFoundException(s"Invalid batchId: $batchId")
      }
    }
  }
}

object BatchesResource {
  val SUPPORTED_BATCH_TYPES = Seq("SPARK")
  val VALID_BATCH_STATES = Seq(
    OperationState.PENDING,
    OperationState.RUNNING,
    OperationState.FINISHED,
    OperationState.ERROR,
    OperationState.CANCELED).map(_.toString)

  def supportedBatchType(batchType: String): Boolean = {
    Option(batchType).exists(bt => SUPPORTED_BATCH_TYPES.contains(bt.toUpperCase(Locale.ROOT)))
  }

  def validBatchState(batchState: String): Boolean = {
    Option(batchState).exists(bt => VALID_BATCH_STATES.contains(bt.toUpperCase(Locale.ROOT)))
  }
}
