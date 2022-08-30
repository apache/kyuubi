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
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.collections.CollectionUtils
import org.apache.ranger.plugin.policyengine.RangerAccessRequest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

import org.apache.kyuubi.plugin.spark.authz.{AccessControlException, ObjectType, _}
import org.apache.kyuubi.plugin.spark.authz.ObjectType._
import org.apache.kyuubi.plugin.spark.authz.ranger.RuleAuthorization.KYUUBI_AUTHZ_TAG
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._

class RuleAuthorization(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case p if !plan.getTagValue(KYUUBI_AUTHZ_TAG).contains(true) =>
      RuleAuthorization.checkPrivileges(spark, p)
      p.setTagValue(KYUUBI_AUTHZ_TAG, true)
      p
    case p => p // do nothing if checked privileges already.
  }
}

object RuleAuthorization {

  val KYUUBI_AUTHZ_TAG = TreeNodeTag[Boolean]("__KYUUBI_AUTHZ_TAG")

  val CONF_FULL_ACCESS_CHECK_ENABLE = "kyuubi.authz.enable.full.access.check"

  def checkPrivileges(spark: SparkSession, plan: LogicalPlan): Unit = {
    val auditHandler = new SparkRangerAuditHandler
    val ugi = getAuthzUgi(spark.sparkContext)
    val opType = OperationType(plan.nodeName)
    val (inputs, outputs) = PrivilegesBuilder.build(plan, spark)
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

    val accessReqs = ArrayBuffer[Seq[RangerAccessRequest]]()
    requests.foreach { request =>
      val resource = request.getResource.asInstanceOf[AccessResource]
      resource.objectType match {
        case ObjectType.COLUMN if resource.getColumns.nonEmpty =>
          val reqs = resource.getColumns.map { col =>
            val cr = AccessResource(COLUMN, resource.getDatabase, resource.getTable, col)
            AccessRequest(cr, ugi, opType, request.accessType).asInstanceOf[RangerAccessRequest]
          }
          accessReqs += reqs
        case _ => accessReqs += Seq(request)
      }
    }
    val isEnabledFullAccessCheck = "true".equalsIgnoreCase(
      spark.sparkContext.getLocalProperty(CONF_FULL_ACCESS_CHECK_ENABLE))
    if (isEnabledFullAccessCheck) {
      verify(
        accessReqs.toStream.flatMap(_.toStream).asJava,
        auditHandler,
        isEnabledFullAccessCheck = true)
    } else {
      accessReqs.foreach {
        reqList =>
          verify(reqList.asJava, auditHandler, isEnabledFullAccessCheck = false)
      }
    }
  }

  @throws[AccessControlException]
  private def verify(
      requests: util.List[RangerAccessRequest],
      auditHandler: SparkRangerAuditHandler,
      isEnabledFullAccessCheck: Boolean): Unit = {
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

    if (!isEnabledFullAccessCheck) {
      val req = disallowedReqs.headOption.orNull
      if (req != null) {
        if (!isEnabledFullAccessCheck) {
          throw new AccessControlException(
            s"Permission denied: user [${req.getUser}]" +
              s" does not have [${req.getAccessType}]" +
              s" privilege on [${req.getResource.getAsString}]")
        }
      }
    } else {
      val accessType2ResMap: mutable.Map[String, mutable.Set[String]] =
        mutable.SortedMap()
      disallowedReqs.foreach { req =>
        {
          val resourceSet = accessType2ResMap.getOrElseUpdate(
            req.getAccessType,
            mutable.SortedSet[String]())
          resourceSet += req.getResource.getAsString
        }
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
}
