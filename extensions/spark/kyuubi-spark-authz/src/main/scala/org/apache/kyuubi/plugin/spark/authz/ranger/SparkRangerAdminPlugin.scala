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

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.commons.collections.CollectionUtils
import org.apache.ranger.plugin.policyengine.RangerAccessRequest
import org.apache.ranger.plugin.service.RangerBasePlugin

import org.apache.kyuubi.plugin.spark.authz.AccessControlException
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._

object SparkRangerAdminPlugin extends RangerBasePlugin("spark", "sparkSql") {

  /**
   * For a Spark SQL query, it may contain 0 or more privilege objects to verify, e.g. a typical
   * JOIN operator may have two tables and their columns to verify.
   *
   * This configuration controls whether to verify the privilege objects in a single RPC call or
   * to verify them one by one.
   */
  def authorizeInSingleCall: Boolean =
    getConfig.getBoolean(s"ranger.plugin.${getServiceType}.authorize.in.single.call", false)

  def getFilterExpr(req: AccessRequest): Option[String] = {
    val result = evalRowFilterPolicies(req, null)
    Option(result)
      .filter(_.isRowFilterEnabled)
      .map(_.getFilterExpr)
      .filter(fe => fe != null && fe.nonEmpty)
  }

  def getMaskingExpr(req: AccessRequest): Option[String] = {
    val col = req.getResource.asInstanceOf[AccessResource].getColumn
    val result = evalDataMaskPolicies(req, null)
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

  @throws[AccessControlException]
  def verify(
      requests: util.List[RangerAccessRequest],
      auditHandler: SparkRangerAuditHandler): Unit = {
    if (CollectionUtils.isEmpty(requests)) {
      return
    }

    val results = SparkRangerAdminPlugin.isAccessAllowed(requests, auditHandler)

    val disallowedReqs = requests.asScala.zip(results.asScala).filter {
      case (_, result) => result != null && !result.getIsAllowed
    } map { case (req, _) => req }
    if (disallowedReqs.isEmpty) {
      return
    }

    val accessType2ResMap: mutable.Map[String, mutable.Set[String]] = mutable.SortedMap()
    disallowedReqs.foreach { req =>
      val resourceSet = accessType2ResMap.getOrElseUpdate(
        req.getAccessType,
        mutable.SortedSet[String]())
      resourceSet += req.getResource.getAsString
    }

    val user: String = disallowedReqs.head.getUser
    val privilegeErrorMsg = accessType2ResMap.map {
      case (accessType, resSet) =>
        s"[${accessType}] privilege on [${resSet.mkString(",")}]"
    }.mkString(", ")
    throw new AccessControlException(
      s"Permission denied: user [${user}] does not have ${privilegeErrorMsg}")
  }
}
