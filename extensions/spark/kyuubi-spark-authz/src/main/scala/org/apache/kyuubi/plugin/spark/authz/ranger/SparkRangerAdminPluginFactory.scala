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

package org.apache.kyuubi.plugin.spark.authz.ranger

import scala.collection.concurrent.{Map, TrieMap}

import org.apache.commons.lang3.StringUtils
import org.apache.ranger.plugin.service.RangerBasePlugin
import org.apache.spark.internal.Logging

object SparkRangerAdminPluginFactory extends Logging {

  val serviceType: String = "spark"
  val defaultAppId: String = "sparkSql"

  val catalog2pluginMap: Map[String, SparkRangerAdminPlugin] = TrieMap()

  init()

  def init(): Unit = {
    getRangerPlugin(None)
  }

  def getRangerPlugin(catalog: Option[String] = None): SparkRangerAdminPlugin = {
    val catalogName = catalog match {
      case Some(name) => name
      case None =>
        "spark_catalog"
    }

    val serviceName = catalogName match {
      case "spark_catalog" =>
        null
      case _ =>
        val defaultPlugin = getRangerPlugin(catalog = None)
        val serviceNameForCatalog = defaultPlugin.getRangerConf
          .get(s"ranger.plugin.spark.catalog.$catalogName.service.name")
        serviceNameForCatalog match {
          case s: String if StringUtils.isBlank(s) =>
            throw new RuntimeException(
              s"config ranger.plugin.spark.catalog.$catalogName.service.name not found")
          case _ => serviceNameForCatalog
        }
    }

    val appId = catalogName match {
      case "spark_catalog" =>
        defaultAppId
      case _ =>
        catalogName
    }

    catalog2pluginMap.getOrElseUpdate(
      catalogName, {
        val plugin = SparkRangerAdminPlugin(serviceName = serviceName, appId = appId)
        plugin
      })
  }

  def getRangerBasePlugin(catalog: Option[String] = None): RangerBasePlugin = {
    getRangerPlugin(catalog).basePlugin
  }
}
