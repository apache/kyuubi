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

package org.apache.spark.ui

import java.util

trait HttpServletRequestLike {

  def getParameter(name: String): String

  def getParameterMap: util.Map[String, Array[String]]

  /**
   * The underlying raw servlet request, which is required when invoking Spark
   * private UI helpers (e.g. UIUtils.headerSparkPage) that take a concrete
   * javax/jakarta HttpServletRequest. Keeping the raw request here, instead of
   * having the adapter re-implement the whole servlet interface, avoids
   * depending on servlet API methods that vary across versions (for example,
   * jakarta.servlet-api 6.0 removed the deprecated isRequestedSessionIdFromUrl
   * and getRealPath methods that 5.0 still declares abstract).
   */
  def underlying: AnyRef
}

object HttpServletRequestLike {
  def fromJavax(req: javax.servlet.http.HttpServletRequest): JavaxHttpServletRequest = {
    new JavaxHttpServletRequest(req)
  }

  def fromJakarta(req: jakarta.servlet.http.HttpServletRequest): JakartaHttpServletRequest = {
    new JakartaHttpServletRequest(req)
  }
}
