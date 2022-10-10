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

import scala.collection.JavaConverters._
import scala.collection.concurrent.{Map, TrieMap}
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}

import org.apache.commons.lang3.StringUtils
import org.apache.ranger.plugin.policyengine.RangerAccessRequest
import org.apache.ranger.plugin.service.RangerBasePlugin

import org.apache.kyuubi.plugin.spark.authz.AccessControlException
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.plugin.spark.authz.util.RangerConfigProvider
import org.apache.kyuubi.plugin.spark.authz.util.RangerConfigProvider.getRangerConfFromPlugin

object SparkRangerAdminPlugin
  extends RangerConfigProvider {

  val serviceType: String = "spark"
  val defaultAppId: String = "sparkSql"

  /**
   * delegated ranger base plugin
   */
  var defaultBasePlugin: RangerBasePlugin = _

  val catalog2pluginMap: Map[String, RangerBasePlugin] = TrieMap()

  /**
   * For a Spark SQL query, it may contain 0 or more privilege objects to verify, e.g. a typical
   * JOIN operator may have two tables and their columns to verify.
   *
   * This configuration controls whether to verify the privilege objects in single call or
   * to verify them one by one.
   */
  def authorizeInSingleCall: Boolean = getRangerConf.getBoolean(
    s"ranger.plugin.${defaultBasePlugin.getServiceType}.authorize.in.single.call",
    false)

  def getFilterExpr(req: AccessRequest): Option[String] = {
    val result = defaultBasePlugin.evalRowFilterPolicies(req, null)
    Option(result)
      .filter(_.isRowFilterEnabled)
      .map(_.getFilterExpr)
      .filter(fe => fe != null && fe.nonEmpty)
  }

  def getMaskingExpr(req: AccessRequest): Option[String] = {
    val col = req.getResource.asInstanceOf[AccessResource].getColumn
    val result = defaultBasePlugin.evalDataMaskPolicies(req, null)
    Option(result).filter(_.isMaskEnabled).map { res =>
      if ("MASK_NULL".equalsIgnoreCase(res.getMaskType)) {
        "NULL"
      } else if ("CUSTOM".equalsIgnoreCase(result.getMaskType)) {
        val maskVal = res.getMaskedValue
        if (maskVal == null) {
          "NULL"
        } else {
          s"${maskVal.replace("{col}", col)}"
        }
      } else if (result.getMaskTypeDef != null) {
        result.getMaskTypeDef.getName match {
          case "MASK" => regexp_replace(col)
          case "MASK_SHOW_FIRST_4" if isSparkVersionAtLeast("3.1") =>
            regexp_replace(col, hasLen = true)
          case "MASK_SHOW_FIRST_4" =>
            val right = regexp_replace(s"substr($col, 5)")
            s"concat(substr($col, 0, 4), $right)"
          case "MASK_SHOW_LAST_4" =>
            val left = regexp_replace(s"left($col, length($col) - 4)")
            s"concat($left, right($col, 4))"
          case "MASK_HASH" => s"md5(cast($col as string))"
          case "MASK_DATE_SHOW_YEAR" => s"date_trunc('YEAR', $col)"
          case _ => result.getMaskTypeDef.getTransformer match {
              case transformer if transformer != null && transformer.nonEmpty =>
                s"${transformer.replace("{col}", col)}"
              case _ => null
            }
        }
      } else {
        null
      }
    }
  }

  private def regexp_replace(expr: String, hasLen: Boolean = false): String = {
    val pos = if (hasLen) ", 5" else ""
    val upper = s"regexp_replace($expr, '[A-Z]', 'X'$pos)"
    val lower = s"regexp_replace($upper, '[a-z]', 'x'$pos)"
    val digits = s"regexp_replace($lower, '[0-9]', 'n'$pos)"
    digits
  }

  /**
   * batch verifying RangerAccessRequests
   * and throws exception with all disallowed privileges
   * for accessType and resources
   */
  def verify(
      requests: Seq[RangerAccessRequest],
      auditHandler: SparkRangerAuditHandler): Unit = {
    if (requests.nonEmpty) {
      val results = defaultBasePlugin.isAccessAllowed(requests.asJava, auditHandler)
      if (results != null) {
        val indices = results.asScala.zipWithIndex.filter { case (result, idx) =>
          result != null && !result.getIsAllowed
        }.map(_._2)
        if (indices.nonEmpty) {
          val user = requests.head.getUser
          val accessTypeToResource =
            indices.foldLeft(LinkedHashMap.empty[String, ArrayBuffer[String]])((m, idx) => {
              val req = requests(idx)
              val accessType = req.getAccessType
              val resource = req.getResource.getAsString
              m.getOrElseUpdate(accessType, ArrayBuffer.empty[String])
                .append(resource)
              m
            })
          val errorMsg = accessTypeToResource
            .map { case (accessType, resources) =>
              s"[$accessType] ${resources.mkString("privilege on [", ",", "]")}"
            }.mkString(", ")
          throw new AccessControlException(
            s"Permission denied: user [$user] does not have $errorMsg")
        }
      }
    }
  }

  def getRangerBasePlugin: RangerBasePlugin = getRangerBasePlugin(None)

  def getRangerBasePlugin(catalog: Option[String]): RangerBasePlugin = {
    catalog match {
      case None | Some("spark_catalog") =>
        defaultBasePlugin
      case Some(catalogName) =>
        val serviceName = {
          val serviceNameForCatalog = getRangerConfFromPlugin(defaultBasePlugin)
            .get(s"ranger.plugin.spark.catalog.$catalogName.service.name")
          if (StringUtils.isNotBlank(serviceNameForCatalog)) {
            serviceNameForCatalog
          } else {
            throw new RuntimeException(
              s"config ranger.plugin.spark.catalog.$catalogName.service.name not found")
          }
        }
        catalog2pluginMap.getOrElseUpdate(
          catalogName,
          initRangerBasePlugin(serviceName = serviceName, appId = catalogName))
    }
  }

  def init(): Unit = {
    defaultBasePlugin = initRangerBasePlugin(serviceName = null, appId = defaultAppId)
  }

  def initRangerBasePlugin(serviceName: String, appId: String): RangerBasePlugin = {
    val basePlugin =
      if (isRanger21orGreater) {
        classOf[RangerBasePlugin].getConstructor(classOf[String], classOf[String], classOf[String])
          .newInstance(serviceType, serviceName, appId)
      } else {
        // ignoring serviceName for Ranger 2.0 and below,
        // fallback to service name in ranger config of serviceType
        classOf[RangerBasePlugin].getConstructor(classOf[String], classOf[String])
          .newInstance(serviceType, appId)
      }
    basePlugin.init()
    basePlugin
  }
}
