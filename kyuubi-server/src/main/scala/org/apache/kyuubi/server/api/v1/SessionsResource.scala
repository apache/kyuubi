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
import javax.ws.rs.{Consumes, DELETE, GET, Path, PathParam, POST, Produces}
import javax.ws.rs.core.{MediaType, Response}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.apache.hive.service.rpc.thrift.{TGetInfoType, TProtocolVersion}

import org.apache.kyuubi.Utils.error
import org.apache.kyuubi.cli.HandleIdentifier
import org.apache.kyuubi.server.api.ApiRequestContext
import org.apache.kyuubi.session.SessionHandle

@Tag(name = "Session")
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class SessionsResource extends ApiRequestContext {

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = "application/json"
    )),
    description = "get all the session list hosted in SessionManager"
  )
  @GET
  def sessionInfoList(): SessionList = {
    SessionList(
      backendService.sessionManager.getSessionList().asScala.map {
        case (handle, session) =>
          SessionOverview(session.user, session.ipAddress, session.createTime, handle)
      }.toSeq
    )
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = "application/json"
    )),
    description = "get a session via session handle identifier"
  )
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

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = "application/json"
    )),
    description =
      "get a information detail via session handle identifier and a specific information type"
  )
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

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = "application/json"
    )),
    description = "get open session count"
  )
  @GET
  @Path("count")
  def sessionCount(): SessionOpenCount = {
    SessionOpenCount(backendService.sessionManager.getOpenSessionCount)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = "application/json"
    )),
    description = "get some statistic info of sessions"
  )
  @GET
  @Path("execPool/statistic")
  def execPoolStatistic(): ExecPoolStatistic = {
    ExecPoolStatistic(backendService.sessionManager.getExecPoolSize,
      backendService.sessionManager.getActiveCount)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = "application/json"
    )),
    description = "Open(create) a Session"
  )
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

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = "application/json"
    )),
    description = "close a Session"
  )
  @DELETE
  @Path("{sessionHandle}")
  def closeSession(@PathParam("sessionHandle") sessionHandleStr: String): Response = {
    val sessionHandle = getSessionHandle(sessionHandleStr)
    backendService.closeSession(sessionHandle)
    Response.ok().build()
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
