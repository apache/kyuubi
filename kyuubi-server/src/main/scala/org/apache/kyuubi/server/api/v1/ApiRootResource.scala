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

import java.net.URI
import javax.ws.rs.{GET, Path, Produces}
import javax.ws.rs.core.{MediaType, Response}

import com.google.common.annotations.VisibleForTesting

import org.apache.kyuubi.server.KyuubiServer
import org.apache.kyuubi.server.api.ApiRequestContext

@Path("/api/v1")
private[v1] class ApiRootResource extends ApiRequestContext {

  @GET
  @Path("ping")
  @Produces(Array(MediaType.TEXT_PLAIN))
  def ping(): String = "pong"

  @Path("sessions")
  def sessions: Class[SessionsResource] = classOf[SessionsResource]

  @GET
  @Path("exception")
  @Produces(Array(MediaType.TEXT_PLAIN))
  @VisibleForTesting
  def test(): Response = {
    1 / 0
    Response.ok().build()
  }

  @GET
  @Path("swagger-ui")
  @Produces(Array(MediaType.TEXT_HTML))
  def swaggerUi(): Response = {
    val serverIP = KyuubiServer.kyuubiServer.frontendServices.head.connectionUrl
    val swaggerUi =
      s"http://$serverIP/swagger-ui-redirected/index.html?url=http://$serverIP/openapi.json"
    Response.temporaryRedirect(new URI(swaggerUi)).build()
  }

}
