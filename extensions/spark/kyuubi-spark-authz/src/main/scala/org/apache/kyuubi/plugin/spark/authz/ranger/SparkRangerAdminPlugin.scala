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

import org.apache.ranger.plugin.service.RangerBasePlugin

object SparkRangerAdminPlugin extends RangerBasePlugin("spark", "sparkSql") {

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
}
