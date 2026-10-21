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
import java.util.Optional
import java.util.regex.Pattern

import scala.collection.JavaConverters._

import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier}

/** Access Paimon's optional view interface without a runtime dependency on Paimon. */
private[spark] object PaimonCatalogUtils {
  private val SUPPORT_VIEW = "org.apache.paimon.spark.catalog.SupportView"

  def getViews(
      catalog: CatalogPlugin,
      namespace: Array[String],
      pattern: Pattern): Seq[(Identifier, String)] = {
    findSupportView(catalog.getClass).toSeq.flatMap { supportView =>
      val listViews = supportView.getMethod("listViews", classOf[Array[String]])
      val loadView = supportView.getMethod("loadView", classOf[Identifier])
      val comment = loadView.getReturnType.getMethod("comment")
      val names = invoke(listViews, catalog, namespace)
        .asInstanceOf[java.util.List[String]]
      names.asScala.filter { name =>
        pattern.matcher(SparkCatalogUtils.quoteIfNeeded(name)).matches()
      }.map { name =>
        val identifier = Identifier.of(namespace, name)
        val view = invoke(loadView, catalog, identifier)
        val remarks = invoke(comment, view).asInstanceOf[Optional[String]].orElse("")
        identifier -> remarks
      }.toSeq
    }
  }

  private def findSupportView(clazz: Class[_]): Option[Class[_]] = {
    if (clazz == null) {
      None
    } else if (clazz.getName == SUPPORT_VIEW) {
      Some(clazz)
    } else {
      clazz.getInterfaces.iterator.map(findSupportView).collectFirst {
        case Some(supportView) => supportView
      }.orElse(findSupportView(clazz.getSuperclass))
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
