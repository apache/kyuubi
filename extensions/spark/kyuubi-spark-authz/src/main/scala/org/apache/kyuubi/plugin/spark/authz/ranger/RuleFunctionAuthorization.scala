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

import scala.collection.mutable

import org.apache.ranger.plugin.policyengine.RangerAccessRequest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.kyuubi.plugin.spark.authz._
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessType.AccessType
import org.apache.kyuubi.plugin.spark.authz.ranger.SparkRangerAdminPlugin._
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._

case class RuleFunctionAuthorization(spark: SparkSession) extends (LogicalPlan => Unit) {
  final val AUTHZ_UDF_KEY: String = "spark.kyuubi.authz.udf.enabled"
  private val authzUDFEnabled: Boolean =
    spark.conf.getOption(AUTHZ_UDF_KEY).exists(_.equalsIgnoreCase("true"))
  override def apply(plan: LogicalPlan): Unit = {
    if (!authzUDFEnabled) {
      return
    }

    val auditHandler = new SparkRangerAuditHandler
    val ugi = getAuthzUgi(spark.sparkContext)
    val (inputs, _, opType) = PrivilegesBuilder.buildFunctions(plan, spark)

    // Use a HashSet to deduplicate the same AccessResource and AccessType, the requests will be all
    // the non-duplicate requests and in the same order as the input requests.
    val requests = new mutable.ArrayBuffer[AccessRequest]()
    val requestsSet = new mutable.HashSet[(AccessResource, AccessType)]()

    def addAccessRequest(objects: Iterable[PrivilegeObject], isInput: Boolean): Unit = {
      objects.foreach { obj =>
        val resource = AccessResource(obj, opType)
        val accessType = ranger.AccessType(obj, opType, isInput)
        if (accessType != AccessType.NONE && !requestsSet.contains((resource, accessType))) {
          requests += AccessRequest(resource, ugi, opType, accessType)
          requestsSet.add(resource, accessType)
        }
      }
    }

    addAccessRequest(inputs, isInput = true)

    val requestSeq: Seq[RangerAccessRequest] =
      requests.map(_.asInstanceOf[RangerAccessRequest]).toSeq

    if (authorizeInSingleCall) {
      verify(requestSeq, auditHandler)
    } else {
      requestSeq.foreach { req =>
        verify(Seq(req), auditHandler)
      }
    }
  }
}
