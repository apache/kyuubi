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

import scala.collection.mutable.ArrayBuffer

import org.apache.ranger.plugin.policyengine.RangerAccessRequest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.kyuubi.plugin.spark.authz.{ranger, ObjectType, OperationType, PrivilegeObject}
import org.apache.kyuubi.plugin.spark.authz.ObjectType.{COLUMN, DATABASE}
import org.apache.kyuubi.plugin.spark.authz.ranger.SparkRangerAdminPlugin.{authorizeInSingleCall, verify}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.getAuthzUgi

trait RuleAuthorizationProvider {

  val privilegeBuilder: (LogicalPlan, SparkSession) => (Seq[PrivilegeObject], Seq[PrivilegeObject])

  val operationTypeBuilder: (String) => OperationType.OperationType = OperationType.apply

  def checkPrivileges(spark: SparkSession, plan: LogicalPlan): Unit = {
    val auditHandler = new SparkRangerAuditHandler
    val ugi = getAuthzUgi(spark.sparkContext)
    val opType = operationTypeBuilder(plan.nodeName)
    val (inputs, outputs) = privilegeBuilder(plan, spark)
    val requests = new ArrayBuffer[AccessRequest]()
    if (inputs.isEmpty && opType == OperationType.SHOWDATABASES) {
      val resource = AccessResource(DATABASE, null)
      requests += AccessRequest(resource, ugi, opType, AccessType.USE)
    }

    def addAccessRequest(objects: Seq[PrivilegeObject], isInput: Boolean): Unit = {
      objects.foreach { obj =>
        val resource = AccessResource(obj, opType)
        val accessType = ranger.AccessType(obj, opType, isInput)
        if (accessType != AccessType.NONE && !requests.exists(o =>
            o.accessType == accessType && o.getResource == resource)) {
          requests += AccessRequest(resource, ugi, opType, accessType)
        }
      }
    }

    addAccessRequest(inputs, isInput = true)
    addAccessRequest(outputs, isInput = false)

    val requestArrays = requests.map { request =>
      val resource = request.getResource.asInstanceOf[AccessResource]
      resource.objectType match {
        case ObjectType.COLUMN if resource.getColumns.nonEmpty =>
          resource.getColumns.map { col =>
            val cr = AccessResource(COLUMN, resource.getDatabase, resource.getTable, col)
            AccessRequest(cr, ugi, opType, request.accessType).asInstanceOf[RangerAccessRequest]
          }
        case _ => Seq(request)
      }
    }

    if (authorizeInSingleCall) {
      verify(requestArrays.flatten, auditHandler)
    } else {
      requestArrays.flatten.foreach { req =>
        verify(Seq(req), auditHandler)
      }
    }
  }
}
