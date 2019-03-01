/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ui

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.KyuubiSparkUtil._

import yaooqinn.kyuubi.utils.ReflectUtils

object KyuubiUIUtils {

  private val className = "org.apache.spark.ui.UIUtils"

  /** Returns a spark page with correctly formatted headers */
  def headerSparkPage(
      request: HttpServletRequest,
      title: String,
      content: => Seq[Node],
      activeTab: SparkUITab): Seq[Node] = {
    val methodMirror = ReflectUtils.reflectStaticMethodScala(className, "headerSparkPage")
    if (equalOrHigherThan("2.4")) {
      methodMirror(request, title, content, activeTab, Some(5000), None, false, false)
        .asInstanceOf[Seq[Node]]
    } else {
      methodMirror(title, content, activeTab, Some(5000), None, false, false)
        .asInstanceOf[Seq[Node]]
    }
  }

  def prependBaseUri(
      request: HttpServletRequest,
      basePath: String = "",
      resource: String = ""): String = {
    val method = ReflectUtils.reflectStaticMethodScala(className, "prependBaseUri")
    if (equalOrHigherThan("2.4")) {
      method(request, basePath, resource).asInstanceOf[String]
    } else {
      method(basePath, resource).asInstanceOf[String]
    }
  }
}
