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

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.util.control.NonFatal

import io.swagger.v3.oas.annotations.media.{ArraySchema, Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.client.api.v1.dto
import org.apache.kyuubi.client.api.v1.dto._
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.operation.{KyuubiOperation, OperationHandle}
import org.apache.kyuubi.server.api.{ApiRequestContext, ApiUtils}
import org.apache.kyuubi.session.{KyuubiSession, SessionHandle}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TGetInfoType, TProtocolVersion}

@Tag(name = "Session")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
private[v1] class SessionsResource extends ApiRequestContext with Logging {
  import ApiUtils.logAndRefineErrorMsg

  implicit def toSessionHandle(str: String): SessionHandle = SessionHandle.fromUUID(str)
  private def sessionManager = fe.be.sessionManager

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      array = new ArraySchema(schema = new Schema(implementation = classOf[SessionData])))),
    description = "get the list of all live sessions")
  @GET
  def sessions(): Seq[SessionData] = {
    sessionManager.allSessions()
      .map(session => ApiUtils.sessionData(session.asInstanceOf[KyuubiSession])).toSeq
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[dto.KyuubiSessionEvent]))),
    description = "get a session event via session handle identifier")
  @GET
  @Path("{sessionHandle}")
  def sessionInfo(@PathParam("sessionHandle") sessionHandleStr: String): dto.KyuubiSessionEvent = {
    try {
      ApiUtils.sessionEvent(sessionManager.getSession(sessionHandleStr).asInstanceOf[KyuubiSession])
    } catch {
      case NonFatal(e) =>
        val errorMsg = s"Invalid $sessionHandleStr"
        throw new NotFoundException(logAndRefineErrorMsg(errorMsg, e))
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[InfoDetail]))),
    description =
      "get a information detail via session handle identifier and a specific information type")
  @GET
  @Path("{sessionHandle}/info/{infoType}")
  def getInfo(
      @PathParam("sessionHandle") sessionHandleStr: String,
      @PathParam("infoType") infoType: Int): InfoDetail = {
    try {
      val info = TGetInfoType.findByValue(infoType)
      val infoValue = fe.be.getInfo(sessionHandleStr, info)
      new InfoDetail(info.toString, infoValue.getStringValue)
    } catch {
      case NonFatal(e) =>
        val errorMsg = s"Unrecognized GetInfoType value: $infoType"
        throw new NotFoundException(logAndRefineErrorMsg(errorMsg, e))
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[SessionOpenCount]))),
    description = "Get the current open session count")
  @GET
  @Path("count")
  def sessionCount(): SessionOpenCount = {
    new SessionOpenCount(sessionManager.getActiveUserSessionCount)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[ExecPoolStatistic]))),
    description = "Get statistic info of background executors")
  @GET
  @Path("execPool/statistic")
  def execPoolStatistic(): ExecPoolStatistic = {
    new ExecPoolStatistic(
      sessionManager.getExecPoolSize,
      sessionManager.getActiveCount,
      sessionManager.getWorkQueueSize)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description = "Open(create) a session")
  @POST
  def openSession(request: SessionOpenRequest): dto.SessionHandle = {
    val userName = fe.getSessionUser(request.getConfigs.asScala.toMap)
    val ipAddress = fe.getIpAddress
    val handle = fe.be.openSession(
      Option(request.getProtocolVersion).map(v => TProtocolVersion.findByValue(v))
        .getOrElse(SessionsResource.DEFAULT_SESSION_PROTOCOL_VERSION),
      userName,
      "",
      ipAddress,
      (request.getConfigs.asScala ++ Map(
        KYUUBI_CLIENT_IP_KEY -> ipAddress,
        KYUUBI_SERVER_IP_KEY -> fe.host,
        KYUUBI_SESSION_CONNECTION_URL_KEY -> fe.connectionUrl,
        KYUUBI_SESSION_REAL_USER_KEY -> fe.getRealUser())).toMap)
    new dto.SessionHandle(handle.identifier, fe.connectionUrl)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description = "Close a session")
  @DELETE
  @Path("{sessionHandle}")
  def closeSession(@PathParam("sessionHandle") sessionHandleStr: String): Response = {
    info(s"Received request of closing $sessionHandleStr")
    fe.be.closeSession(sessionHandleStr)
    Response.ok().build()
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[OperationHandle]))),
    description = "Create an operation with EXECUTE_STATEMENT type")
  @POST
  @Path("{sessionHandle}/operations/statement")
  def executeStatement(
      @PathParam("sessionHandle") sessionHandleStr: String,
      request: StatementRequest): OperationHandle = {
    try {
      fe.be.executeStatement(
        sessionHandleStr,
        request.getStatement,
        request.getConfOverlay.asScala.toMap,
        request.isRunAsync,
        request.getQueryTimeout)
    } catch {
      case NonFatal(e) =>
        val errorMsg = "Error executing statement"
        throw new NotFoundException(logAndRefineErrorMsg(errorMsg, e))
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[OperationHandle]))),
    description = "Create an operation with GET_TYPE_INFO type")
  @POST
  @Path("{sessionHandle}/operations/typeInfo")
  def getTypeInfo(@PathParam("sessionHandle") sessionHandleStr: String): OperationHandle = {
    try {
      fe.be.getTypeInfo(sessionHandleStr)
    } catch {
      case NonFatal(e) =>
        val errorMsg = "Error getting type information"
        throw new NotFoundException(logAndRefineErrorMsg(errorMsg, e))
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[OperationHandle]))),
    description = "Create an operation with GET_CATALOGS type")
  @POST
  @Path("{sessionHandle}/operations/catalogs")
  def getCatalogs(@PathParam("sessionHandle") sessionHandleStr: String): OperationHandle = {
    try {
      fe.be.getCatalogs(sessionHandleStr)
    } catch {
      case NonFatal(e) =>
        val errorMsg = "Error getting catalogs"
        throw new NotFoundException(logAndRefineErrorMsg(errorMsg, e))
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[OperationHandle]))),
    description = "Create an operation with GET_SCHEMAS type")
  @POST
  @Path("{sessionHandle}/operations/schemas")
  def getSchemas(
      @PathParam("sessionHandle") sessionHandleStr: String,
      request: GetSchemasRequest): OperationHandle = {
    try {
      val operationHandle = fe.be.getSchemas(
        sessionHandleStr,
        request.getCatalogName,
        request.getSchemaName)
      operationHandle
    } catch {
      case NonFatal(e) =>
        val errorMsg = "Error getting schemas"
        throw new NotFoundException(logAndRefineErrorMsg(errorMsg, e))
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[OperationHandle]))),
    description = "Create an operation with GET_TABLES type")
  @POST
  @Path("{sessionHandle}/operations/tables")
  def getTables(
      @PathParam("sessionHandle") sessionHandleStr: String,
      request: GetTablesRequest): OperationHandle = {
    try {
      fe.be.getTables(
        sessionHandleStr,
        request.getCatalogName,
        request.getSchemaName,
        request.getTableName,
        request.getTableTypes)
    } catch {
      case NonFatal(e) =>
        val errorMsg = "Error getting tables"
        throw new NotFoundException(logAndRefineErrorMsg(errorMsg, e))
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[OperationHandle]))),
    description = "Create an operation with GET_TABLE_TYPES type")
  @POST
  @Path("{sessionHandle}/operations/tableTypes")
  def getTableTypes(@PathParam("sessionHandle") sessionHandleStr: String): OperationHandle = {
    try {
      fe.be.getTableTypes(sessionHandleStr)
    } catch {
      case NonFatal(e) =>
        val errorMsg = "Error getting table types"
        throw new NotFoundException(logAndRefineErrorMsg(errorMsg, e))
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[OperationHandle]))),
    description = "Create an operation with GET_COLUMNS type")
  @POST
  @Path("{sessionHandle}/operations/columns")
  def getColumns(
      @PathParam("sessionHandle") sessionHandleStr: String,
      request: GetColumnsRequest): OperationHandle = {
    try {
      fe.be.getColumns(
        sessionHandleStr,
        request.getCatalogName,
        request.getSchemaName,
        request.getTableName,
        request.getColumnName)
    } catch {
      case NonFatal(e) =>
        val errorMsg = "Error getting columns"
        throw new NotFoundException(logAndRefineErrorMsg(errorMsg, e))
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[OperationHandle]))),
    description = "Create an operation with GET_FUNCTIONS type")
  @POST
  @Path("{sessionHandle}/operations/functions")
  def getFunctions(
      @PathParam("sessionHandle") sessionHandleStr: String,
      request: GetFunctionsRequest): OperationHandle = {
    try {
      fe.be.getFunctions(
        sessionHandleStr,
        request.getCatalogName,
        request.getSchemaName,
        request.getFunctionName)
    } catch {
      case NonFatal(e) =>
        val errorMsg = "Error getting functions"
        throw new NotFoundException(logAndRefineErrorMsg(errorMsg, e))
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[OperationHandle]))),
    description = "Create an operation with GET_PRIMARY_KEY type")
  @POST
  @Path("{sessionHandle}/operations/primaryKeys")
  def getPrimaryKeys(
      @PathParam("sessionHandle") sessionHandleStr: String,
      request: GetPrimaryKeysRequest): OperationHandle = {
    try {
      fe.be.getPrimaryKeys(
        sessionHandleStr,
        request.getCatalogName,
        request.getSchemaName,
        request.getTableName)
    } catch {
      case NonFatal(e) =>
        val errorMsg = "Error getting primary keys"
        throw new NotFoundException(logAndRefineErrorMsg(errorMsg, e))
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[OperationHandle]))),
    description = "Create an operation with GET_CROSS_REFERENCE type")
  @POST
  @Path("{sessionHandle}/operations/crossReference")
  def getCrossReference(
      @PathParam("sessionHandle") sessionHandleStr: String,
      request: GetCrossReferenceRequest): OperationHandle = {
    try {
      fe.be.getCrossReference(
        sessionHandleStr,
        request.getPrimaryCatalog,
        request.getPrimarySchema,
        request.getPrimaryTable,
        request.getForeignCatalog,
        request.getForeignSchema,
        request.getForeignTable)
    } catch {
      case NonFatal(e) =>
        val errorMsg = "Error getting cross reference"
        throw new NotFoundException(logAndRefineErrorMsg(errorMsg, e))
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      array = new ArraySchema(schema = new Schema(implementation =
        classOf[OperationData])))),
    description =
      "get the list of all type operations belong to session")
  @GET
  @Path("{sessionHandle}/operations")
  def getOperation(@PathParam("sessionHandle") sessionHandleStr: String): Seq[OperationData] = {
    try {
      fe.be.sessionManager.operationManager.allOperations().map { operation =>
        if (StringUtils.equalsIgnoreCase(
            operation.getSession.handle.identifier.toString,
            sessionHandleStr)) {
          ApiUtils.operationData(operation.asInstanceOf[KyuubiOperation])
        }
      }.toSeq.asInstanceOf[Seq[OperationData]]
    } catch {
      case NonFatal(e) =>
        val errorMsg = "Error getting the list of all type operations belong to session"
        throw new NotFoundException(logAndRefineErrorMsg(errorMsg, e))
    }
  }
}

object SessionsResource {
  final val DEFAULT_SESSION_PROTOCOL_VERSION = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1
}
