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

import java.util.UUID
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hive.service.rpc.thrift.{TGetInfoType, TProtocolVersion}

import org.apache.kyuubi.Utils.error
import org.apache.kyuubi.cli.HandleIdentifier
import org.apache.kyuubi.operation.OperationHandle
import org.apache.kyuubi.server.api.ApiRequestContext
import org.apache.kyuubi.session.SessionHandle

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class SessionsResource extends ApiRequestContext {

  @GET
  def sessionInfoList(): SessionList = {
    SessionList(
      backendService.sessionManager.getSessionList().asScala.map {
        case (handle, session) =>
          SessionOverview(session.user, session.ipAddress, session.createTime, handle)
      }.toList
    )
  }

  @GET
  @Path("{sessionHandle}")
  def sessionInfo(@PathParam("sessionHandle") sessionHandleStr: String): SessionDetail = {
    val sessionHandle = getSessionHandle(sessionHandleStr)

    try {
      val session = backendService.sessionManager.getSession(sessionHandle)
      SessionDetail(session.user, session.ipAddress, session.createTime, sessionHandle,
        session.lastAccessTime, session.lastIdleTime, session.getNoOperationTime, session.conf)
    } catch {
      case NonFatal(e) =>
        error(s"Invalid $sessionHandle", e)
        throw new NotFoundException(s"Invalid $sessionHandle")
    }
  }

  @GET
  @Path("{sessionHandle}/info/{infoType}")
  def getInfo(@PathParam("sessionHandle") sessionHandleStr: String,
              @PathParam("infoType") infoType: Int): InfoDetail = {
    val sessionHandle = getSessionHandle(sessionHandleStr)
    val info = TGetInfoType.findByValue(infoType)

    try {
      val infoValue = backendService.getInfo(sessionHandle, info)
      InfoDetail(info.toString, infoValue.getStringValue)
    } catch {
      case NonFatal(e) =>
        error(s"Unrecognized GetInfoType value: $infoType", e)
        throw new NotFoundException(s"Unrecognized GetInfoType value: $infoType")
    }
  }

  @GET
  @Path("count")
  def sessionCount(): SessionOpenCount = {
    SessionOpenCount(backendService.sessionManager.getOpenSessionCount)
  }

  @GET
  @Path("execPool/statistic")
  def execPoolStatistic(): ExecPoolStatistic = {
    ExecPoolStatistic(backendService.sessionManager.getExecPoolSize,
      backendService.sessionManager.getActiveCount)
  }

  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def openSession(request: SessionOpenRequest): SessionHandle = {
    backendService.openSession(
      TProtocolVersion.findByValue(request.protocolVersion),
      request.user,
      request.password,
      request.ipAddr,
      request.configs)
  }

  @DELETE
  @Path("{sessionHandle}")
  def closeSession(@PathParam("sessionHandle") sessionHandleStr: String): Response = {
    val sessionHandle = getSessionHandle(sessionHandleStr)
    backendService.closeSession(sessionHandle)
    Response.ok().build()
  }

  @GET
  @Path("{sessionHandle}/executeStatement")
  def executeStatement(@PathParam("sessionHandle") sessionHandleStr: String,
                       @QueryParam("statement") statement: String,
                       @QueryParam("runAsync") runAsync: String,
                       @QueryParam("queryTimeout") queryTimeout: String): OperationHandle = {
    checkQueryParams(statement, runAsync, queryTimeout)
    val sessionHandle = getSessionHandle(sessionHandleStr)
    try {
      backendService.executeStatement(sessionHandle, statement, runAsync.toBoolean,
        queryTimeout.toLong)
    } catch {
      case NonFatal(_) =>
        throw new NotFoundException(s"Error executing statement")
    }
  }

  @GET
  @Path("{sessionHandle}/getTypeInfo")
  def getTypeInfo(@PathParam("sessionHandle") sessionHandleStr: String): OperationHandle = {
    val sessionHandle = getSessionHandle(sessionHandleStr)
    try {
      backendService.getTypeInfo(sessionHandle)
    } catch {
      case NonFatal(_) =>
        throw new NotFoundException(s"Error getting typeInfo")
    }
  }

  @GET
  @Path("{sessionHandle}/getCatalogs")
  def getCatalogs(@PathParam("sessionHandle") sessionHandleStr: String): OperationHandle = {
    val sessionHandle = getSessionHandle(sessionHandleStr)
    try {
      backendService.getCatalogs(sessionHandle)
    } catch {
      case NonFatal(_) =>
        throw new NotFoundException(s"Error getting catalogs")
    }
  }

  @GET
  @Path("{sessionHandle}/getSchemas")
  def getSchemas(@PathParam("sessionHandle") sessionHandleStr: String,
                 @QueryParam("catalogName") catalogName: String,
                 @QueryParam("schemaName") schemaName: String): OperationHandle = {
    checkQueryParams(catalogName, schemaName, catalogName)
    val sessionHandle = getSessionHandle(sessionHandleStr)
    try {
      backendService.getSchemas(sessionHandle, catalogName, schemaName)
    } catch {
      case NonFatal(_) =>
        throw new NotFoundException(s"Error getting schemas")
    }
  }

  @GET
  @Path("{sessionHandle}/getTables")
  def getTables(@PathParam("sessionHandle") sessionHandleStr: String,
                @QueryParam("catalogName") catalogName: String,
                @QueryParam("schemaName") schemaName: String,
                @QueryParam("tableName") tableName: String,
                @QueryParam("tableTypes") tableTypes: String): OperationHandle = {
    checkQueryParams(catalogName, schemaName, tableName, tableTypes)
    val sessionHandle = getSessionHandle(sessionHandleStr)
    try {
      val tableList: java.util.List[String] = tableTypes.split(",").toList.asJava
      backendService.getTables(sessionHandle, catalogName, schemaName, tableName, tableList)
    } catch {
      case NonFatal(_) =>
        throw new NotFoundException(s"Error getting tables")
    }
  }

  @GET
  @Path("{sessionHandle}/getTableTypes")
  def getTableTypes(@PathParam("sessionHandle") sessionHandleStr: String): OperationHandle = {
    val sessionHandle = getSessionHandle(sessionHandleStr)
    try {
      backendService.getTableTypes(sessionHandle)
    } catch {
      case NonFatal(_) =>
        throw new NotFoundException(s"Error getting tableTypes")
    }
  }

  @GET
  @Path("{sessionHandle}/getColumns")
  def getColumns(@PathParam("sessionHandle") sessionHandleStr: String,
                 @QueryParam("catalogName") catalogName: String,
                 @QueryParam("schemaName") schemaName: String,
                 @QueryParam("tableName") tableName: String,
                 @QueryParam("columnName") columnName: String): OperationHandle = {
    checkQueryParams(catalogName, schemaName, tableName, columnName)
    val sessionHandle = getSessionHandle(sessionHandleStr)
    try {
      backendService.getColumns(sessionHandle, catalogName, schemaName, tableName, columnName)
    } catch {
      case NonFatal(_) =>
        throw new NotFoundException(s"Error getting columns")
    }
  }

  @GET
  @Path("{sessionHandle}/getFunctions")
  def getFunctions(@PathParam("sessionHandle") sessionHandleStr: String,
                   @QueryParam("catalogName") catalogName: String,
                   @QueryParam("schemaName") schemaName: String,
                   @QueryParam("functionName") functionName: String): OperationHandle = {
    checkQueryParams(catalogName, schemaName, functionName)
    val sessionHandle = getSessionHandle(sessionHandleStr)
    try {
      backendService.getFunctions(sessionHandle, catalogName, schemaName, functionName)
    } catch {
      case NonFatal(_) =>
        throw new NotFoundException(s"Error getting functions")
    }
  }

  def getSessionHandle(sessionHandleStr: String): SessionHandle = {
    try {
      val splitSessionHandle = sessionHandleStr.split("\\|")
      val handleIdentifier = new HandleIdentifier(
        UUID.fromString(splitSessionHandle(0)), UUID.fromString(splitSessionHandle(1)))
      val protocolVersion = TProtocolVersion.findByValue(splitSessionHandle(2).toInt)
      val sessionHandle = new SessionHandle(handleIdentifier, protocolVersion)

      // if the sessionHandle is invalid, KyuubiSQLException will be thrown here.
      backendService.sessionManager.getSession(sessionHandle)
      sessionHandle
    } catch {
      case NonFatal(e) =>
        error(s"Error getting sessionHandle by $sessionHandleStr.", e)
        throw new NotFoundException(s"Error getting sessionHandle by $sessionHandleStr.")
    }
  }

  def checkQueryParams(params: String*): Unit = {
    if (params.contains(null)) {
      throw new NotFoundException(s"Params can't be null")
    }
  }
}
