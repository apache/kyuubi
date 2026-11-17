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

package org.apache.kyuubi.engine.spark.util

import java.lang.reflect.{InvocationTargetException, Method}
import java.util.regex.Pattern

import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, TableCatalog}

/** Access Spark 4.2's ViewCatalog while retaining compatibility with older Spark runtimes. */
private[spark] object SparkViewCatalogUtils {
  private val VIEW_CATALOG = "org.apache.spark.sql.connector.catalog.ViewCatalog"

  def isViewCatalog(catalog: CatalogPlugin): Boolean =
    findViewCatalog(catalog.getClass).nonEmpty

  def getViews(
      catalog: CatalogPlugin,
      namespace: Array[String],
      pattern: Pattern): Seq[(Identifier, String)] = {
    findViewCatalog(catalog.getClass).toSeq.flatMap { viewCatalog =>
      val listViews = viewCatalog.getMethod("listViews", classOf[Array[String]])
      val loadView = viewCatalog.getMethod("loadView", classOf[Identifier])
      val properties = loadView.getReturnType.getMethod("properties")
      val identifiers = invoke(listViews, catalog, namespace).asInstanceOf[Array[Identifier]]
      identifiers.filter { ident =>
        pattern.matcher(SparkCatalogUtils.quoteIfNeeded(ident.name())).matches()
      }.map { ident =>
        val view = invoke(loadView, catalog, ident)
        val metadata = invoke(properties, view).asInstanceOf[java.util.Map[String, String]]
        ident -> metadata.getOrDefault(TableCatalog.PROP_COMMENT, "")
      }.toSeq
    }
  }

  private def findViewCatalog(clazz: Class[_]): Option[Class[_]] = {
    if (clazz == null) {
      None
    } else if (clazz.getName == VIEW_CATALOG) {
      Some(clazz)
    } else {
      clazz.getInterfaces.iterator.map(findViewCatalog).collectFirst {
        case Some(viewCatalog) => viewCatalog
      }.orElse(findViewCatalog(clazz.getSuperclass))
    }
  }

  private def invoke(method: Method, target: AnyRef, arguments: AnyRef*): AnyRef = {
    try {
      method.invoke(target, arguments: _*)
    } catch {
      case e: InvocationTargetException => throw e.getCause
    }
  }
}
