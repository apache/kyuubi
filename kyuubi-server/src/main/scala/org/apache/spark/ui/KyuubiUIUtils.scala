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

object KyuubiUIUtils {

  /** Returns a spark page with correctly formatted headers */
  def headerSparkPage(
      request: HttpServletRequest,
      title: String,
      content: => Seq[Node],
      activeTab: SparkUITab,
      refreshInterval: Option[Int] = None,
      helpText: Option[String] = None,
      showVisualization: Boolean = false,
      useDataTables: Boolean = false): Seq[Node] = {
    if (equalOrHigherThan("2.4")) {
      val method = UIUtils.getClass.getMethod(
        "headerSparkPage",
        request.getClass,
        title.getClass,
        content.getClass,
        activeTab.getClass,
        refreshInterval.getClass,
        helpText.getClass,
        java.lang.Boolean.TYPE,
        java.lang.Boolean.TYPE)
      method.invoke(null,
        request,
        title,
        content,
        activeTab,
        refreshInterval,
        helpText,
        new java.lang.Boolean(showVisualization),
        new java.lang.Boolean(useDataTables)).asInstanceOf[Seq[Node]]

    } else {
      val method = UIUtils.getClass.getMethod(
        "headerSparkPage",
        classOf[String],
        classOf[() => Seq[Node]],
        classOf[SparkUITab],
        refreshInterval.getClass,
        helpText.getClass,
        java.lang.Boolean.TYPE,
        java.lang.Boolean.TYPE)
      method.invoke(null,
        title,
        content,
        activeTab,
        refreshInterval,
        helpText,
        new java.lang.Boolean(showVisualization),
        new java.lang.Boolean(useDataTables)).asInstanceOf[Seq[Node]]
    }
  }

  def prependBaseUri(
      request: HttpServletRequest,
      basePath: String = "",
      resource: String = ""): String = {
    if (equalOrHigherThan("2.4")) {
      val method = UIUtils.getClass.getMethod("prependBaseUri",
        classOf[HttpServletRequest], classOf[String], classOf[String])
      method.invoke(null, request, basePath, resource).asInstanceOf[String]
    } else {
      val method = UIUtils.getClass.getMethod("prependBaseUri",
        classOf[String], classOf[String])
      method.invoke(null, basePath, resource).asInstanceOf[String]
    }
  }

}
