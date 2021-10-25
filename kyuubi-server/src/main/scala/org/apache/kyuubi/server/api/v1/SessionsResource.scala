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

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.cli.HandleIdentifier
import org.apache.kyuubi.server.api.ApiRequestContext
import org.apache.kyuubi.session.SessionHandle

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class SessionsResource extends ApiRequestContext {

  @GET
  def sessionInfoList(): SessionInfoList = {
    SessionInfoList(backendService.sessionManager.getSessionList()
      .map(session => SessionInfo(session.user, session.ipAddress, session.createTime))
    )
  }

  @GET
  @Path("{sessionHandle}")
  def sessionInfo(@PathParam("sessionHandle") sessionHandleStr: String): SessionInfo = {
    val splitSessionHandle = sessionHandleStr.split("\\|")
    val handleIdentifier = new HandleIdentifier(
      UUID.fromString(splitSessionHandle(0)), UUID.fromString(splitSessionHandle(1)))
    val protocolVersion = TProtocolVersion.findByValue(splitSessionHandle(2).toInt)
    val sessionHandle = new SessionHandle(handleIdentifier, protocolVersion)

    val session = backendService.sessionManager.getSession(sessionHandle)

    SessionInfo(session.user, session.ipAddress, session.createTime,
      session.lastAccessTime, session.lastIdleTime, session.getNoOperationTime, session.conf)
  }

  @GET
  @Path("count")
  def sessionCount(): SessionOpenCount = {
    SessionOpenCount(backendService.sessionManager.getOpenSessionCount)
  }

  @GET
  @Path("execpool/statistic")
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
    val splitSessionHandle = sessionHandleStr.split("\\|")
    val handleIdentifier = new HandleIdentifier(
      UUID.fromString(splitSessionHandle(0)), UUID.fromString(splitSessionHandle(1)))
    val protocolVersion = TProtocolVersion.findByValue(splitSessionHandle(2).toInt)
    val sessionHandle = new SessionHandle(handleIdentifier, protocolVersion)
    backendService.closeSession(sessionHandle)
    Response.ok().build()
  }
}
