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
import org.apache.kyuubi.operation.{OperationHandle, OperationType}
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

  @POST
  @Path("{sessionHandle}/operations")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def operations(@PathParam("sessionHandle") sessionHandleStr: String,
    request: OperationRequest): OperationHandle = {
    val (operationType, addition) = try {
      (OperationType.withName(request.operation), request.addition)
    } catch {
      case NonFatal(_) =>
        throw new NotFoundException(s"Unsupported Operation type: ${request.operation}")
    }

    val sessionHandle = getSessionHandle(sessionHandleStr)
    try {
      val manager = backendService.sessionManager
      manager.operationManager.getOperationHandle(
        manager.getSession(sessionHandle), operationType, addition)
    } catch {
      case NonFatal(_) =>
        throw new NotFoundException(s"Error getting OperationHandle, " +
          s"addition: ${request.addition.mkString(",")}.")
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
}
