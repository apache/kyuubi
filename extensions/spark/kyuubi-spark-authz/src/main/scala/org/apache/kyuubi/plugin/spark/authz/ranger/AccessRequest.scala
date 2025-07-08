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

import java.util.{HashMap => JHashMap, Set => JSet}
import java.util.Date

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.hadoop.security.UserGroupInformation
import org.apache.ranger.plugin.policyengine.{RangerAccessRequestImpl, RangerPolicyEngine}
import org.apache.spark.internal.Logging

import org.apache.kyuubi.plugin.spark.authz.OperationType.OperationType
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessType._
import org.apache.kyuubi.util.reflect.DynMethods
import org.apache.kyuubi.util.reflect.DynMethods.{BoundMethod, UnboundMethod}
import org.apache.kyuubi.util.reflect.ReflectUtils._

case class AccessRequest private (accessType: AccessType) extends RangerAccessRequestImpl

object AccessRequest extends Logging {
  def apply(
      resource: AccessResource,
      user: UserGroupInformation,
      opType: OperationType,
      accessType: AccessType): AccessRequest = {
    val userName = user.getShortUserName
    val userGroups = getUserGroups(user)
    val req = new AccessRequest(accessType)
    req.setResource(resource)
    req.setUser(userName)
    req.setUserGroups(userGroups)
    req.setAction(opType.toString)
    try {
      (getRolesFromUserAndGroupsMethod zip setUserRolesMethod).foreach {
        case (getMethod, setMethod) =>
          val roles = getMethod.invoke[JSet[String]](userName, userGroups)
          setMethod.bind(req).invoke[Unit](roles)
      }
    } catch {
      case _: Exception =>
    }
    req.setAccessTime(new Date())
    accessType match {
      case USE => req.setAccessType(RangerPolicyEngine.ANY_ACCESS)
      case _ => req.setAccessType(accessType.toString.toLowerCase)
    }
    try {
      val clusterName = invokeAs[String](SparkRangerAdminPlugin, "getClusterName")
      invokeAs[Unit](req, "setClusterName", (classOf[String], clusterName))
    } catch {
      case _: Exception =>
    }
    req
  }

  private val getRolesFromUserAndGroupsMethod: Option[BoundMethod] = Try {
    DynMethods.builder("getRolesFromUserAndGroups")
      .impl(SparkRangerAdminPlugin.getClass, classOf[String], classOf[JSet[String]])
      .build(SparkRangerAdminPlugin)
  }.toOption

  private val setUserRolesMethod: Option[UnboundMethod] = Try {
    DynMethods.builder("setUserRoles")
      .impl(classOf[AccessRequest], classOf[JSet[String]])
      .buildChecked()
  }.toOption

  private def getUserGroupsFromUgi(user: UserGroupInformation): JSet[String] = {
    user.getGroupNames.toSet.asJava
  }

  private lazy val getUserStoreEnricherMethod: Option[BoundMethod] = Try {
    DynMethods.builder("getUserStoreEnricher")
      .impl(SparkRangerAdminPlugin.getClass)
      .build(SparkRangerAdminPlugin)
  }.toOption

  private lazy val getRangerUserStoreMethod: Option[UnboundMethod] = Try {
    DynMethods.builder("getRangerUserStore")
      .impl("org.apache.ranger.plugin.contextenricher.RangerUserStoreEnricher")
      .build()
  }.toOption

  private lazy val getUserGroupMappingMethod: Option[UnboundMethod] = Try {
    DynMethods.builder("getUserGroupMapping")
      .impl("org.apache.ranger.plugin.util.RangerUserStore")
      .build()
  }.toOption

  private def getUserGroupsFromUserStore(user: UserGroupInformation): Option[JSet[String]] = {
    (getUserStoreEnricherMethod, getRangerUserStoreMethod, getUserGroupMappingMethod) match {
      case (Some(getEnricher), Some(getUserStore), Some(getMapping)) => Try {
          val enricher = getEnricher.invoke()
          val userStore = getUserStore.bind(enricher).invoke()
          val userGroupMapping: JHashMap[String, JSet[String]] = getMapping.bind(userStore).invoke()
          userGroupMapping.get(user.getShortUserName)
        }.toOption
      case _ =>
        val svcType = SparkRangerAdminPlugin.getServiceType
        val confKey = s"ranger.plugin.$svcType.use.usergroups.from.userstore.enabled"
        logWarning(s"Unable to use Ranger's userstore though '$confKey' is true.")
        None
    }
  }

  private def getUserGroups(user: UserGroupInformation): JSet[String] = {
    if (SparkRangerAdminPlugin.useUserGroupsFromUserStoreEnabled) {
      getUserGroupsFromUserStore(user).getOrElse(getUserGroupsFromUgi(user))
    } else {
      getUserGroupsFromUgi(user)
    }
  }

}
