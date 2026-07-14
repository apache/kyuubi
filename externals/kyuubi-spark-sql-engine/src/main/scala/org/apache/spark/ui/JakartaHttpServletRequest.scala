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

import jakarta.servlet.http._

/**
 * Adapts a [[jakarta.servlet.http.HttpServletRequest]] to the engine UI code via
 * [[HttpServletRequestLike]]. Only the request parameters used by the engine UI
 * are forwarded; Spark private UI helpers that need a real servlet request are
 * given [[HttpServletRequestLike.underlying]] instead.
 */
class JakartaHttpServletRequest(req: HttpServletRequest)
  extends HttpServletRequestLike {
  override def getParameter(name: String): String = req.getParameter(name)

  override def getParameterMap: util.Map[String, Array[String]] = req.getParameterMap

  override def underlying: AnyRef = req
}
