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

package org.apache.kyuubi.server.ui

import java.net.URL
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import org.eclipse.jetty.servlet.{DefaultServlet, ServletContextHandler, ServletHolder}

import org.apache.kyuubi.KyuubiException

private[kyuubi] object JettyUtils {

  /**
   * Create a handler for serving files from a static directory
   *
   * @param resourceBase the resource directory contains static resource files
   * @param contextPath the content path to set for the handler
   * @return a static [[ServletContextHandler]]
   */
  def createStaticHandler(
      resourceBase: String,
      contextPath: String): ServletContextHandler = {
    val contextHandler = new ServletContextHandler()
    val holder = new ServletHolder(classOf[DefaultServlet])
    Option(Thread.currentThread().getContextClassLoader.getResource(resourceBase)) match {
      case Some(res) =>
        holder.setInitParameter("resourceBase", res.toString)
      case None =>
        throw new KyuubiException("Could not find resource path for Web UI: " + resourceBase)
    }
    contextHandler.setContextPath(contextPath)
    contextHandler.addServlet(holder, "/")
    contextHandler
  }

  def createServletHandler(contextPath: String, servlet: HttpServlet): ServletContextHandler = {
    val handler = new ServletContextHandler()
    val holder = new ServletHolder(servlet)
    handler.setContextPath(contextPath)
    handler.addServlet(holder, "/")
    handler
  }

  def createRedirectHandler(src: String, dest: String): ServletContextHandler = {
    val redirectedServlet = new HttpServlet {
      private def doReq(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        val newURL = new URL(new URL(req.getRequestURL.toString), dest).toString
        resp.sendRedirect(newURL)
      }
      override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        doReq(req, resp)
      }

      override def doPut(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        doReq(req, resp)
      }

      override def doPost(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        doReq(req, resp)
      }

      override def doDelete(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        doReq(req, resp)
      }

      override protected def doTrace(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
      }
    }

    createServletHandler(src, redirectedServlet)

  }
}
