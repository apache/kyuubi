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

package org.apache.kyuubi.server.trino.api.v1

import javax.ws.rs.{GET, Path, Produces}
import javax.ws.rs.core.{MediaType, Response}

import com.google.common.annotations.VisibleForTesting
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.responses.ApiResponse
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.servlet.ServletContainer

import org.apache.kyuubi.KYUUBI_VERSION
import org.apache.kyuubi.client.api.v1.dto._
import org.apache.kyuubi.server.KyuubiTrinoFrontendService
import org.apache.kyuubi.server.trino.api.{ApiRequestContext, FrontendServiceContext, TrinoServerConfig}

@Path("/v1")
private[v1] class ApiRootResource extends ApiRequestContext {

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON)),
    description = "Get the version of Kyuubi server.")
  @GET
  @Path("version")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def version(): VersionInfo = new VersionInfo(KYUUBI_VERSION)

  @GET
  @Path("ping")
  @Produces(Array(MediaType.TEXT_PLAIN))
  def ping(): String = "pong"

  @Path("statement")
  def admin: Class[StatementResource] = classOf[StatementResource]

  @GET
  @Path("exception")
  @Produces(Array(MediaType.TEXT_PLAIN))
  @VisibleForTesting
  def test(): Response = {
    1 / 0
    Response.ok().build()
  }
}

private[server] object ApiRootResource {

  def getServletHandler(fe: KyuubiTrinoFrontendService): ServletContextHandler = {
    val openapiConf: ResourceConfig = new TrinoServerConfig
    val holder = new ServletHolder(new ServletContainer(openapiConf))
    val handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    FrontendServiceContext.set(handler, fe)
    handler.addServlet(holder, "/*")
    handler
  }
}
