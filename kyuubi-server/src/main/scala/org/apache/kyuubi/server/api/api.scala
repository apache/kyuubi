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

package org.apache.kyuubi.server.api

import javax.servlet.ServletContext
import javax.servlet.http.HttpServletRequest
import javax.ws.rs.core.Context

import org.eclipse.jetty.server.handler.ContextHandler
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.glassfish.jersey.server.ServerProperties
import org.glassfish.jersey.servlet.ServletContainer

import org.apache.kyuubi.service.BackendService

private[api] trait ApiRequestContext {

  @Context
  protected var servletContext: ServletContext = _

  @Context
  protected var httpRequest: HttpServletRequest = _

  def backendService: BackendService = BackendServiceProvider.getBackendService(servletContext)

}

private[api] object BackendServiceProvider {

  private val attribute = getClass.getCanonicalName

  def setBackendService(contextHandler: ContextHandler, be: BackendService): Unit = {
    contextHandler.setAttribute(attribute, be)
  }

  def getBackendService(context: ServletContext): BackendService = {
    context.getAttribute(attribute).asInstanceOf[BackendService]
  }
}

private[server] object ApiUtils {

  def getServletHandler(backendService: BackendService): ServletContextHandler = {
    val servlet = new ServletHolder(classOf[ServletContainer])
    servlet.setInitParameter(
      ServerProperties.PROVIDER_PACKAGES,
      "org.apache.kyuubi.server.api.v1")
    servlet.setInitParameter(
      ServerProperties.PROVIDER_CLASSNAMES,
      classOf[KyuubiScalaObjectMapper].getName)
    val handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    BackendServiceProvider.setBackendService(handler, backendService)
    handler.setContextPath("/api")
    handler.addServlet(servlet, "/*")
    handler
  }
}
