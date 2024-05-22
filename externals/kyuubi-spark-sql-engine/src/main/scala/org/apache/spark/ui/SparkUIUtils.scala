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

import java.lang.{Boolean => JBoolean}

import scala.xml.Node

import org.apache.kyuubi.engine.spark.KyuubiSparkUtil.SPARK_ENGINE_RUNTIME_VERSION
import org.apache.kyuubi.util.reflect.DynMethods

/**
 * A place to invoke non-public APIs of [[UIUtils]], anything to be added here need to
 * think twice
 */
object SparkUIUtils {

  def formatDuration(ms: Long): String = {
    UIUtils.formatDuration(ms)
  }

  def headerSparkPage(
      request: HttpServletRequestLike,
      title: String,
      content: Seq[Node],
      activeTab: SparkUITab,
      helpText: Option[String] = None,
      showVisualization: JBoolean = false,
      useDataTables: JBoolean = false): Seq[Node] = {
    val headerSparkPageMethod = if (SPARK_ENGINE_RUNTIME_VERSION >= "4.0") {
      DynMethods.builder("headerSparkPage")
        .impl(
          UIUtils.getClass,
          classOf[jakarta.servlet.http.HttpServletRequest],
          classOf[String],
          classOf[() => Seq[Node]],
          classOf[SparkUITab],
          classOf[Option[String]],
          classOf[Boolean],
          classOf[Boolean])
        .buildChecked(UIUtils)
    } else {
      DynMethods.builder("headerSparkPage")
        .impl(
          UIUtils.getClass,
          classOf[javax.servlet.http.HttpServletRequest],
          classOf[String],
          classOf[() => Seq[Node]],
          classOf[SparkUITab],
          classOf[Option[String]],
          classOf[Boolean],
          classOf[Boolean])
        .buildChecked(UIUtils)
    }
    headerSparkPageMethod.invoke[Seq[Node]](
      request,
      title,
      () => content,
      activeTab,
      helpText,
      showVisualization,
      useDataTables)
  }

  def prependBaseUri(
      request: HttpServletRequestLike,
      basePath: String = "",
      resource: String = ""): String = {
    val prependBaseUriMethod = if (SPARK_ENGINE_RUNTIME_VERSION >= "4.0") {
      DynMethods.builder("prependBaseUri")
        .impl(
          UIUtils.getClass,
          classOf[jakarta.servlet.http.HttpServletRequest],
          classOf[String],
          classOf[String])
        .buildChecked(UIUtils)
    } else {
      DynMethods.builder("prependBaseUri")
        .impl(
          UIUtils.getClass,
          classOf[javax.servlet.http.HttpServletRequest],
          classOf[String],
          classOf[String])
        .buildChecked(UIUtils)
    }
    prependBaseUriMethod.invoke[String](request, basePath, resource)
  }
}
