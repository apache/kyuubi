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

package org.apache.spark.kyuubi.ui

import java.net.URL
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import org.sparkproject.jetty.servlet.{ServletContextHandler, ServletHolder}

// the code forked from `org.apache.spark.ui.JettyUtils`
object KyuubiUIUtils {
  def createServletHandler(
      path: String,
      servlet: HttpServlet,
      basePath: String): ServletContextHandler = {
    val prefixedPath = if (basePath == "" && path == "/") {
      path
    } else {
      (basePath + path).stripSuffix("/")
    }
    val contextHandler = new ServletContextHandler
    val holder = new ServletHolder(servlet)
    contextHandler.setContextPath(prefixedPath)
    contextHandler.addServlet(holder, "/")
    contextHandler
  }

  def createRedirectHandler(
      srcPath: String,
      destPath: String,
      beforeRedirect: HttpServletRequest => Unit = x => (),
      basePath: String = "",
      httpMethods: Set[String] = Set("GET")): ServletContextHandler = {
    val prefixedDestPath = basePath + destPath
    val servlet = new HttpServlet {
      override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        if (httpMethods.contains("GET")) {
          doRequest(request, response)
        } else {
          response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
        }
      }
      override def doPost(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        if (httpMethods.contains("POST")) {
          doRequest(request, response)
        } else {
          response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
        }
      }
      private def doRequest(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        beforeRedirect(request)
        // Make sure we don't end up with "//" in the middle
        val newUrl = new URL(new URL(request.getRequestURL.toString), prefixedDestPath).toString
        response.sendRedirect(newUrl)
      }
      // SPARK-5983 ensure TRACE is not supported
      protected override def doTrace(req: HttpServletRequest, res: HttpServletResponse): Unit = {
        res.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
      }
    }
    createServletHandler(srcPath, servlet, basePath)
  }
}
