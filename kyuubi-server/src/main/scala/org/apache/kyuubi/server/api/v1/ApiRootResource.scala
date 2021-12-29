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
import org.eclipse.jetty.servlet.{DefaultServlet, ServletContextHandler, ServletHolder}
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.servlet.ServletContainer

import org.apache.kyuubi.server.KyuubiRestFrontendService
import org.apache.kyuubi.server.api.{ApiRequestContext, FrontendServiceContext, OpenAPIConfig}

@Path("/api/v1")
private[v1] class ApiRootResource extends ApiRequestContext {

  @GET
  @Path("ping")
  @Produces(Array(MediaType.TEXT_PLAIN))
  def ping(): String = "pong"

  @Path("sessions")
  def sessions: Class[SessionsResource] = classOf[SessionsResource]

  @Path("operations")
  def operations: Class[OperationsResource] = classOf[OperationsResource]

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
  def swaggerUI(): Response = {
    val swaggerUI = s"http://${fe.connectionUrl}/swagger-ui-redirected/index.html?url=" +
      s"http://${fe.connectionUrl}/openapi.json"
    Response.temporaryRedirect(new URI(swaggerUI)).build()
  }

}

private[server] object ApiRootResource {

  def getServletHandler(fe: KyuubiRestFrontendService): ServletContextHandler = {
    val openapiConf: ResourceConfig = new OpenAPIConfig
    val servlet = new ServletHolder(new ServletContainer(openapiConf))
    val handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    FrontendServiceContext.set(handler, fe)
    handler.addServlet(servlet, "/*")

    // install swagger-ui, these static files are copied from
    // https://github.com/swagger-api/swagger-ui/tree/master/dist
    val swaggerUI = new ServletHolder("swagger-ui", classOf[DefaultServlet])
    swaggerUI.setInitParameter(
      "resourceBase",
      getClass.getClassLoader()
        .getResource("META-INF/resources/webjars/swagger-ui/4.1.3/")
        .toExternalForm)
    swaggerUI.setInitParameter("pathInfoOnly", "true")
    handler.addServlet(swaggerUI, "/swagger-ui-redirected/*");
    handler
  }
}
