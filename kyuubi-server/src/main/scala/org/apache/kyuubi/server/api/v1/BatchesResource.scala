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

import java.io.InputStream
import java.nio.file.{Files, Path => JPath}
import java.util
import java.util.{Collections, Locale, UUID}
import java.util.concurrent.ConcurrentHashMap
import javax.ws.rs._
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.apache.commons.lang3.StringUtils
import org.glassfish.jersey.media.multipart.{FormDataContentDisposition, FormDataMultiPart, FormDataParam}

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.client.api.v1.dto._
import org.apache.kyuubi.client.exception.KyuubiRestException
import org.apache.kyuubi.client.util.BatchUtils._
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.engine._
import org.apache.kyuubi.operation.{BatchJobSubmission, FetchOrientation, OperationState}
import org.apache.kyuubi.server.api.ApiRequestContext
import org.apache.kyuubi.server.api.v1.BatchesResource._
import org.apache.kyuubi.server.metadata.MetadataManager
import org.apache.kyuubi.server.metadata.api.{Metadata, MetadataFilter}
import org.apache.kyuubi.session.{KyuubiBatchSession, KyuubiSessionManager, SessionHandle, SessionType}
import org.apache.kyuubi.util.JdbcUtils

@Tag(name = "Batch")
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class BatchesResource extends ApiRequestContext with Logging {
  private val internalRestClients = new ConcurrentHashMap[String, InternalRestClient]()
  private lazy val internalSocketTimeout =
    fe.getConf.get(BATCH_INTERNAL_REST_CLIENT_SOCKET_TIMEOUT).toInt
  private lazy val internalConnectTimeout =
    fe.getConf.get(BATCH_INTERNAL_REST_CLIENT_CONNECT_TIMEOUT).toInt
  private lazy val internalRequestMaxAttempts =
    fe.getConf.get(BATCH_INTERNAL_REST_CLIENT_REQUEST_MAX_ATTEMPTS)
  private lazy val internalRequestAttemptWait =
    fe.getConf.get(BATCH_INTERNAL_REST_CLIENT_REQUEST_ATTEMPT_WAIT).toInt
  private lazy val internalSecurityEnabled =
    fe.getConf.get(ENGINE_SECURITY_ENABLED)
  private lazy val resourceFileMaxSize = fe.getConf.get(BATCH_RESOURCE_FILE_MAX_SIZE)
  private lazy val extraResourceFileMaxSize = fe.getConf.get(BATCH_EXTRA_RESOURCE_FILE_MAX_SIZE)
  private lazy val metadataSearchWindow = fe.getConf.get(METADATA_SEARCH_WINDOW)
  private lazy val batchInfoInternalRedirect = fe.getConf.get(BATCH_INFO_INTERNAL_REDIRECT)

  private def batchV2Enabled(reqConf: Map[String, String]): Boolean = {
    fe.getConf.get(BATCH_SUBMITTER_ENABLED) &&
    reqConf.getOrElse(BATCH_IMPL_VERSION.key, fe.getConf.get(BATCH_IMPL_VERSION)) == "2"
  }

  private def getInternalRestClient(kyuubiInstance: String): InternalRestClient = {
    internalRestClients.computeIfAbsent(
      kyuubiInstance,
      kyuubiInstance =>
        new InternalRestClient(
          kyuubiInstance,
          fe.getConf.get(FRONTEND_PROXY_HTTP_CLIENT_IP_HEADER),
          internalSocketTimeout,
          internalConnectTimeout,
          internalSecurityEnabled,
          internalRequestMaxAttempts,
          internalRequestAttemptWait))
  }

  private def sessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]

  private def buildBatch(session: KyuubiBatchSession): Batch = {
    val batchOp = session.batchJobSubmissionOp
    val batchOpStatus = batchOp.getStatus

    val (name, appId, appUrl, appState, appDiagnostic) = batchOp.getApplicationInfo.map { appInfo =>
      val name = Option(batchOp.batchName).getOrElse(appInfo.name)
      (name, appInfo.id, appInfo.url.orNull, appInfo.state.toString, appInfo.error.orNull)
    }.getOrElse {
      sessionManager.getBatchMetadata(batchOp.batchId) match {
        case Some(batch) =>
          val diagnostic = batch.engineError.orNull
          (batchOp.batchName, batch.engineId, batch.engineUrl, batch.engineState, diagnostic)
        case None =>
          (batchOp.batchName, null, null, null, null)
      }
    }

    new Batch(
      batchOp.batchId,
      session.user,
      batchOp.batchType,
      name,
      batchOp.appStartTime,
      appId,
      appUrl,
      appState,
      appDiagnostic,
      session.connectionUrl,
      batchOpStatus.state.toString,
      session.createTime,
      batchOpStatus.completed,
      Map.empty[String, String].asJava)
  }

  private def buildBatch(
      metadata: Metadata,
      batchAppStatus: Option[ApplicationInfo]): Batch = {
    batchAppStatus.map { appStatus =>
      val currentBatchState =
        if (BatchJobSubmission.applicationFailed(batchAppStatus)) {
          OperationState.ERROR.toString
        } else if (BatchJobSubmission.applicationTerminated(batchAppStatus)) {
          OperationState.FINISHED.toString
        } else if (batchAppStatus.isDefined) {
          OperationState.RUNNING.toString
        } else {
          metadata.state
        }

      val name = Option(metadata.requestName).getOrElse(appStatus.name)
      val appId = appStatus.id
      val appUrl = appStatus.url.orNull
      val appState = appStatus.state.toString
      val appDiagnostic = appStatus.error.orNull

      new Batch(
        metadata.identifier,
        metadata.username,
        metadata.engineType,
        name,
        metadata.engineOpenTime,
        appId,
        appUrl,
        appState,
        appDiagnostic,
        metadata.kyuubiInstance,
        currentBatchState,
        metadata.createTime,
        metadata.endTime,
        Map.empty[String, String].asJava)
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
    openBatchSessionInternal(request)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[Batch]))),
    description = "create and open a batch session with uploading resource file")
  @POST
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  def openBatchSessionWithUpload(
      @FormDataParam("batchRequest") batchRequest: BatchRequest,
      @FormDataParam("resourceFile") resourceFileInputStream: InputStream,
      @FormDataParam("resourceFile") resourceFileMetadata: FormDataContentDisposition,
      formDataMultiPart: FormDataMultiPart): Batch = {
    require(
      fe.getConf.get(BATCH_RESOURCE_UPLOAD_ENABLED),
      "Batch resource upload function is disabled.")
    require(
      batchRequest != null,
      "batchRequest is required and please check the content type" +
        " of batchRequest is application/json")

    val unUploadedExtraResourceFileNames =
      batchRequest.getExtraResourcesMap.values.asScala.flatMap(_.split(",")).toSet.diff(
        formDataMultiPart.getFields.values.flatten.map(_.getContentDisposition.getFileName).toSet)
        .filter(StringUtils.isNotBlank(_))
    require(
      unUploadedExtraResourceFileNames.isEmpty,
      f"required extra resource files " +
        f"[${unUploadedExtraResourceFileNames.toList.sorted.mkString(",")}]" +
        f" are not uploaded in the multipart form data")

    openBatchSessionInternal(
      batchRequest,
      isResourceFromUpload = true,
      resourceFileInputStream = Some(resourceFileInputStream),
      resourceFileMetadata = Some(resourceFileMetadata),
      formDataMultiPartOpt = Some(formDataMultiPart))
  }

  /**
   * open new batch session with request
   *
   * @param request              instance of BatchRequest
   * @param isResourceFromUpload whether to clean up temporary uploaded resource file
   *                             in local path after execution
   */
  private def openBatchSessionInternal(
      request: BatchRequest,
      isResourceFromUpload: Boolean = false,
      resourceFileInputStream: Option[InputStream] = None,
      resourceFileMetadata: Option[FormDataContentDisposition] = None,
      formDataMultiPartOpt: Option[FormDataMultiPart] = None): Batch = {
    require(
      supportedBatchType(request.getBatchType),
      s"${request.getBatchType} is not in the supported list: $SUPPORTED_BATCH_TYPES}")
    require(request.getResource != null, "resource is a required parameter")
    if (request.getBatchType.equalsIgnoreCase("SPARK")) {
      require(request.getClassName != null, "classname is a required parameter for SPARK")
    }
    request.setBatchType(request.getBatchType.toUpperCase(Locale.ROOT))

    val userName = fe.getSessionUser(request.getConf.asScala.toMap)
    val ipAddress = fe.getIpAddress
    val userProvidedBatchId = request.getConf.asScala.get(KYUUBI_BATCH_ID_KEY)
    userProvidedBatchId.foreach { batchId =>
      try UUID.fromString(batchId)
      catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(s"$KYUUBI_BATCH_ID_KEY=$batchId must be an UUID", e)
      }
    }

    userProvidedBatchId.flatMap { batchId =>
      sessionManager.getBatchFromMetadataStore(batchId)
    } match {
      case Some(batch) =>
        markDuplicated(batch)
      case None =>
        val batchId = userProvidedBatchId.getOrElse(UUID.randomUUID().toString)
        if (isResourceFromUpload) {
          handleUploadingFiles(
            batchId,
            request,
            resourceFileInputStream.get,
            resourceFileMetadata.get.getFileName,
            formDataMultiPartOpt)
        }
        request.setConf(
          (request.getConf.asScala ++ Map(
            KYUUBI_BATCH_ID_KEY -> batchId,
            KYUUBI_BATCH_RESOURCE_UPLOADED_KEY -> isResourceFromUpload.toString,
            KYUUBI_CLIENT_IP_KEY -> ipAddress,
            KYUUBI_SERVER_IP_KEY -> fe.host,
            KYUUBI_SESSION_CONNECTION_URL_KEY -> fe.connectionUrl,
            KYUUBI_SESSION_REAL_USER_KEY -> fe.getRealUser())).asJava)

        if (batchV2Enabled(request.getConf.asScala.toMap)) {
          logger.info(s"Submit batch job $batchId using Batch API v2")
          return Try {
            sessionManager.initializeBatchState(
              userName,
              ipAddress,
              request.getConf.asScala.toMap,
              request)
          } match {
            case Success(batchId) =>
              sessionManager.getBatchFromMetadataStore(batchId) match {
                case Some(batch) => batch
                case None => throw new IllegalStateException(
                    s"can not find batch $batchId from metadata store")
              }
            case Failure(cause) if JdbcUtils.isDuplicatedKeyDBErr(cause) =>
              sessionManager.getBatchFromMetadataStore(batchId) match {
                case Some(batch) => markDuplicated(batch)
                case None => throw new IllegalStateException(
                    s"can not find duplicated batch $batchId from metadata store")
              }
            case Failure(cause) => throw new IllegalStateException(cause)
          }
        }

        Try {
          sessionManager.openBatchSession(
            userName,
            "anonymous",
            ipAddress,
            request)
        } match {
          case Success(sessionHandle) =>
            sessionManager.getBatchSession(sessionHandle) match {
              case Some(batchSession) => buildBatch(batchSession)
              case None => throw new IllegalStateException(
                  s"can not find batch $batchId from metadata store")
            }
          case Failure(cause) if JdbcUtils.isDuplicatedKeyDBErr(cause) =>
            sessionManager.getBatchFromMetadataStore(batchId) match {
              case Some(batch) => markDuplicated(batch)
              case None => throw new IllegalStateException(
                  s"can not find duplicated batch $batchId from metadata store")
            }
          case Failure(cause) => throw new IllegalStateException(cause)
        }
    }
  }

  private def markDuplicated(batch: Batch): Batch = {
    warn(s"duplicated submission: ${batch.getId}, ignore and return the existing batch.")
    batch.setBatchInfo(Map(KYUUBI_BATCH_DUPLICATED_KEY -> "true").asJava)
    batch
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
    val userName = fe.getSessionUser(Map.empty[String, String])
    val sessionHandle = formatSessionHandle(batchId)
    sessionManager.getBatchSession(sessionHandle).map { batchSession =>
      buildBatch(batchSession)
    }.getOrElse {
      sessionManager.getBatchMetadata(batchId).map { metadata =>
        val isOperationTerminated = (StringUtils.isNotBlank(metadata.state)
          && OperationState.isTerminal(OperationState.withName(metadata.state)))
        val isApplicationTerminated = (StringUtils.isNotBlank(metadata.engineState)
          && ApplicationState.isTerminated(ApplicationState.withName(metadata.engineState)))

        if (!batchInfoInternalRedirect ||
          batchV2Enabled(metadata.requestConf) ||
          isOperationTerminated ||
          isApplicationTerminated ||
          metadata.kyuubiInstance == fe.connectionUrl) {
          MetadataManager.buildBatch(metadata)
        } else {
          val internalRestClient = getInternalRestClient(metadata.kyuubiInstance)
          try {
            internalRestClient.getBatch(userName, fe.getIpAddress, batchId)
          } catch {
            case e: KyuubiRestException =>
              error(s"Error redirecting get batch[$batchId] to ${metadata.kyuubiInstance}", e)
              val batchAppStatus = sessionManager.applicationManager.getApplicationInfo(
                metadata.appMgrInfo,
                batchId,
                Some(userName),
                // prevent that the batch be marked as terminated if application state is NOT_FOUND
                Some(metadata.engineOpenTime).filter(_ > 0).orElse(Some(System.currentTimeMillis)))
              // if the batch app is terminated, update the metadata in db.
              if (BatchJobSubmission.applicationTerminated(batchAppStatus)) {
                val appInfo = batchAppStatus.get
                sessionManager.updateMetadata(Metadata(
                  identifier = batchId,
                  engineId = appInfo.id,
                  engineName = appInfo.name,
                  engineUrl = appInfo.url.orNull,
                  engineState = appInfo.state.toString,
                  engineError = appInfo.error))
              }
              buildBatch(metadata, batchAppStatus)
          }
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
      @QueryParam("batchName") batchName: String,
      @QueryParam("createTime") createTime: Long,
      @QueryParam("endTime") endTime: Long,
      @QueryParam("from") from: Int,
      @QueryParam("size") @DefaultValue("100") size: Int,
      @QueryParam("desc") @DefaultValue("false") desc: Boolean): GetBatchesResponse = {
    require(
      createTime >= 0 && endTime >= 0 && (endTime == 0 || createTime <= endTime),
      "Invalid time range")
    if (batchState != null) {
      require(
        validBatchState(batchState),
        s"The valid batch state can be one of the following: ${VALID_BATCH_STATES.mkString(",")}")
    }

    val createTimeFilter =
      math.max(createTime, metadataSearchWindow.map(System.currentTimeMillis() - _).getOrElse(0L))
    val filter = MetadataFilter(
      sessionType = SessionType.BATCH,
      engineType = batchType,
      username = batchUser,
      state = batchState,
      requestName = batchName,
      createTime = createTimeFilter,
      endTime = endTime)
    val batches = sessionManager.getBatchesFromMetadataStore(filter, from, size, desc)
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
      @QueryParam("size") @DefaultValue("100") size: Int): OperationLog = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    val sessionHandle = formatSessionHandle(batchId)
    sessionManager.getBatchSession(sessionHandle).map { batchSession =>
      try {
        val submissionOp = batchSession.batchJobSubmissionOp
        val rowSet = submissionOp.getOperationLogRowSet(FetchOrientation.FETCH_NEXT, from, size)
        val columns = rowSet.getColumns
        val logRowSet: util.List[String] =
          if (columns == null || columns.size == 0) {
            Collections.emptyList()
          } else {
            assert(columns.size == 1)
            columns.get(0).getStringVal.getValues
          }
        new OperationLog(logRowSet, logRowSet.size)
      } catch {
        case NonFatal(e) =>
          val errorMsg = s"Error getting operation log for batchId: $batchId"
          error(errorMsg, e)
          throw new NotFoundException(errorMsg)
      }
    }.getOrElse {
      sessionManager.getBatchMetadata(batchId).map { metadata =>
        if (batchV2Enabled(metadata.requestConf) && metadata.state == "INITIALIZED") {
          info(s"Batch $batchId is waiting for scheduling")
          val dummyLogs = List(s"Batch $batchId is waiting for scheduling").asJava
          new OperationLog(dummyLogs, dummyLogs.size)
        } else if (fe.connectionUrl != metadata.kyuubiInstance) {
          val internalRestClient = getInternalRestClient(metadata.kyuubiInstance)
          internalRestClient.getBatchLocalLog(userName, fe.getIpAddress, batchId, from, size)
        } else if (batchV2Enabled(metadata.requestConf) &&
          // in batch v2 impl, the operation state is changed from PENDING to RUNNING
          // before being added to SessionManager.
          (metadata.state == "PENDING" || metadata.state == "RUNNING")) {
          info(s"Batch $batchId is waiting for submitting")
          val dummyLogs = List(s"Batch $batchId is waiting for submitting").asJava
          new OperationLog(dummyLogs, dummyLogs.size)
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
  def closeBatchSession(@PathParam("batchId") batchId: String): CloseBatchResponse = {
    def forceKill(
        appMgrInfo: ApplicationManagerInfo,
        batchId: String,
        user: String): KillResponse = {
      val (killed, message) = sessionManager.applicationManager
        .killApplication(appMgrInfo, batchId, Some(user))
      info(s"Mark batch[$batchId] closed by ${fe.connectionUrl}")
      sessionManager.updateMetadata(Metadata(identifier = batchId, peerInstanceClosed = true))
      (killed, message)
    }

    val sessionHandle = formatSessionHandle(batchId)
    sessionManager.getBatchSession(sessionHandle).map { batchSession =>
      val userName = fe.getSessionUser(batchSession.user)
      val ipAddress = fe.getIpAddress
      batchSession.batchJobSubmissionOp.withOperationLog {
        warn(s"Received kill batch request from $userName/$ipAddress")
      }
      sessionManager.closeSession(batchSession.handle)
      val (killed, msg) = batchSession.batchJobSubmissionOp.getKillMessage
      new CloseBatchResponse(killed, msg)
    }.getOrElse {
      sessionManager.getBatchMetadata(batchId).map { metadata =>
        fe.getSessionUser(metadata.username)
        if (OperationState.isTerminal(OperationState.withName(metadata.state))) {
          new CloseBatchResponse(false, s"The batch[$metadata] has been terminated.")
        } else if (batchV2Enabled(metadata.requestConf) && metadata.state == "INITIALIZED" &&
          // there is a chance that metadata is outdated, then `cancelUnscheduledBatch` fails
          // and returns false
          fe.batchService.get.cancelUnscheduledBatch(batchId)) {
          new CloseBatchResponse(true, s"Unscheduled batch $batchId is canceled.")
        } else if (batchV2Enabled(metadata.requestConf) && metadata.kyuubiInstance == null) {
          // code goes here indicates metadata is outdated, recursively calls itself to refresh
          // the metadata
          closeBatchSession(batchId)
        } else if (metadata.kyuubiInstance != fe.connectionUrl) {
          info(s"Redirecting delete batch[$batchId] to ${metadata.kyuubiInstance}")
          val internalRestClient = getInternalRestClient(metadata.kyuubiInstance)
          try {
            internalRestClient.deleteBatch(metadata.username, fe.getIpAddress, batchId)
          } catch {
            case e: KyuubiRestException =>
              error(s"Error redirecting delete batch[$batchId] to ${metadata.kyuubiInstance}", e)
              val (killed, msg) = forceKill(metadata.appMgrInfo, batchId, metadata.username)
              new CloseBatchResponse(killed, if (killed) msg else Utils.stringifyException(e))
          }
        } else { // should not happen, but handle this for safe
          warn(s"Something wrong on deleting batch[$batchId], try forcibly killing application")
          val (killed, msg) = forceKill(metadata.appMgrInfo, batchId, metadata.username)
          new CloseBatchResponse(killed, msg)
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
      schema = new Schema(implementation = classOf[ReassignBatchResponse]))),
    description = "Reassign batches to a new instance")
  @POST
  @Path("/reassign")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def reassignBatchSessions(request: ReassignBatchRequest): ReassignBatchResponse = {
    val url = fe.connectionUrl
    val kyuubiInstance = request.getKyuubiInstance
    if (kyuubiInstance == url) {
      throw new IllegalStateException(s"KyuubiInstance is alive: $kyuubiInstance")
    }
    val internalRestClient = getInternalRestClient(kyuubiInstance)
    if (!Utils.isTesting && internalRestClient.pingAble()) {
      throw new IllegalStateException(s"KyuubiInstance is alive: $kyuubiInstance")
    }
    try {
      val batchIds = sessionManager.reassignBatchSessions(kyuubiInstance, fe.connectionUrl)
      val recoveredBatchIds = fe.recoverBatchSessionsFromReassign(batchIds)
      new ReassignBatchResponse(recoveredBatchIds.asJava, kyuubiInstance, url)
    } catch {
      case e: Exception =>
        throw new Exception(s"Error reassign batches from $kyuubiInstance to $url", e)
    }
  }

  private def handleUploadingFiles(
      batchId: String,
      request: BatchRequest,
      resourceFileInputStream: InputStream,
      resourceFileName: String,
      formDataMultiPartOpt: Option[FormDataMultiPart]): Option[JPath] = {
    val uploadFileFolderPath = KyuubiApplicationManager.sessionUploadFolderPath(batchId)
    try {
      handleUploadingResourceFile(
        request,
        resourceFileInputStream,
        resourceFileName,
        uploadFileFolderPath)
      handleUploadingExtraResourcesFiles(request, formDataMultiPartOpt, uploadFileFolderPath)
      Some(uploadFileFolderPath)
    } catch {
      case e: Exception =>
        Utils.deleteDirectoryRecursively(uploadFileFolderPath.toFile)
        throw e
    }
  }

  private def handleUploadingResourceFile(
      request: BatchRequest,
      inputStream: InputStream,
      fileName: String,
      uploadFileFolderPath: JPath): Unit = {
    try {
      val tempFile = Utils.writeToTempFile(inputStream, uploadFileFolderPath, fileName)
      if (resourceFileMaxSize > 0 && Files.size(tempFile.toPath) > resourceFileMaxSize) {
        throw new RuntimeException(
          s"Resource file $fileName exceeds the maximum size limit $resourceFileMaxSize bytes")
      }
      fe.sessionManager.tempFileService.addPathToExpiration(tempFile.toPath)
      request.setResource(tempFile.getPath)
    } catch {
      case e: Exception =>
        throw new RuntimeException(
          s"Failed handling uploaded resource file $fileName: ${e.getMessage}",
          e)
    }
  }

  private def handleUploadingExtraResourcesFiles(
      request: BatchRequest,
      formDataMultiPartOpt: Option[FormDataMultiPart],
      uploadFileFolderPath: JPath): Unit = {
    val extraResourceMap = request.getExtraResourcesMap.asScala
    if (extraResourceMap.nonEmpty) {
      val fileNameSeparator = ","
      val formDataMultiPart = formDataMultiPartOpt.get
      val transformedExtraResourcesMap = extraResourceMap
        .mapValues(confValue =>
          confValue.split(fileNameSeparator).filter(StringUtils.isNotBlank(_)))
        .filter { case (confKey, fileNames) =>
          fileNames.nonEmpty && StringUtils.isNotBlank(confKey)
        }.mapValues { fileNames =>
          fileNames.map(fileName =>
            Option(formDataMultiPart.getField(fileName))
              .getOrElse(throw new RuntimeException(s"File part for file $fileName not found")))
        }.map {
          case (confKey, fileParts) =>
            val tempFilePaths = fileParts.map { filePart =>
              val fileName = filePart.getContentDisposition.getFileName
              try {
                val tempFile = Utils.writeToTempFile(
                  filePart.getValueAs(classOf[InputStream]),
                  uploadFileFolderPath,
                  fileName)
                if (extraResourceFileMaxSize > 0
                  && Files.size(tempFile.toPath) > extraResourceFileMaxSize) {
                  throw new RuntimeException(
                    s"Extra resource file $fileName exceeds the maximum size limit " +
                      s"$extraResourceFileMaxSize bytes")
                }
                fe.sessionManager.tempFileService.addPathToExpiration(tempFile.toPath)
                tempFile.getPath
              } catch {
                case e: Exception =>
                  throw new RuntimeException(
                    s"Failed handling uploaded extra resource file $fileName: ${e.getMessage}",
                    e)
              }
            }
            (confKey, tempFilePaths.mkString(fileNameSeparator))
        }

      val conf = request.getConf
      transformedExtraResourcesMap.foreach { case (confKey, tempFilePathStr) =>
        conf.get(confKey) match {
          case confValue: String if StringUtils.isNotBlank(confValue) =>
            conf.put(confKey, List(confValue.trim, tempFilePathStr).mkString(fileNameSeparator))
          case _ => conf.put(confKey, tempFilePathStr)
        }
      }
    }
  }
}

object BatchesResource {
  private lazy val SUPPORTED_BATCH_TYPES = Set("SPARK", "PYSPARK")
  private lazy val VALID_BATCH_STATES = Set(
    OperationState.PENDING,
    OperationState.RUNNING,
    OperationState.FINISHED,
    OperationState.ERROR,
    OperationState.CANCELED).map(_.toString)

  private def supportedBatchType(batchType: String): Boolean = {
    Option(batchType).exists(bt => SUPPORTED_BATCH_TYPES.contains(bt.toUpperCase(Locale.ROOT)))
  }

  private def validBatchState(batchState: String): Boolean = {
    Option(batchState).exists(bt => VALID_BATCH_STATES.contains(bt.toUpperCase(Locale.ROOT)))
  }

  def batchResourceUploadFolderPath(sessionId: String): JPath = {
    require(StringUtils.isNotBlank(sessionId))
    KyuubiApplicationManager.uploadWorkDir.resolve(sessionId)
  }
}
