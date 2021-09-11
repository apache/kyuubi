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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import javax.ws.rs.{Consumes, GET, Path, POST, Produces}
import javax.ws.rs.core.MediaType
import org.apache.hive.service.rpc.thrift.TProtocolVersion
import scala.collection.JavaConverters.mapAsScalaMapConverter

import org.apache.kyuubi.server.api.ApiRequestContext
import org.apache.kyuubi.server.api.v1.dto.{SessionOpenedCount, SessionOpenRequest}

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class SessionsResource extends ApiRequestContext {

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  @GET
  @Path("count")
  def sessionCount(): SessionOpenedCount = {
    val sessionOpenedCount = new SessionOpenedCount()
    sessionOpenedCount.setOpenSessionCount(
      backendService.sessionManager.getOpenSessionCount
    )
    sessionOpenedCount
  }

  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def openSession(request : SessionOpenRequest): String = {
    val sessionHandle = backendService.openSession(
      TProtocolVersion.findByValue(request.getProtocolVersion),
      request.getUser,
      request.getPassword,
      request.getIpAddr,
      request.getConfigs.asScala.toMap)
    mapper.writeValueAsString(sessionHandle)
  }
}
