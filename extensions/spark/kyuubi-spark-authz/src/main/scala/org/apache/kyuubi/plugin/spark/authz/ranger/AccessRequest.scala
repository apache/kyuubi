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

import org.apache.hadoop.security.UserGroupInformation
import org.apache.ranger.plugin.policyengine.{RangerAccessRequestImpl, RangerPolicyEngine}

import org.apache.kyuubi.plugin.spark.authz.OperationType.OperationType
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessType._
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.{invoke, invokeAs}

case class AccessRequest private (accessType: AccessType) extends RangerAccessRequestImpl

object AccessRequest {
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
      val roles = invokeAs[JSet[String]](
        SparkRangerAdminPlugin,
        "getRolesFromUserAndGroups",
        (classOf[String], userName),
        (classOf[JSet[String]], userGroups))
      invoke(req, "setUserRoles", (classOf[JSet[String]], roles))
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
      invoke(req, "setClusterName", (classOf[String], clusterName))
    } catch {
      case _: Exception =>
    }
    req
  }

  private def getUserGroupsFromUgi(user: UserGroupInformation): JSet[String] = {
    user.getGroupNames.toSet.asJava
  }

  private def getUserGroupsFromUserStore(user: UserGroupInformation): Option[JSet[String]] = {
    try {
      val storeEnricher = invoke(SparkRangerAdminPlugin, "getUserStoreEnricher")
      val userStore = invoke(storeEnricher, "getRangerUserStore")
      val userGroupMapping =
        invokeAs[JHashMap[String, JSet[String]]](userStore, "getUserGroupMapping")
      Some(userGroupMapping.get(user.getShortUserName))
    } catch {
      case _: NoSuchMethodException =>
        None
    }
  }

  private def getUserGroups(user: UserGroupInformation): JSet[String] = {
    if (SparkRangerAdminPlugin.useUserGroupsFromUserStoreEnabled) {
      getUserGroupsFromUserStore(user)
        .getOrElse(getUserGroupsFromUgi(user))
    } else {
      getUserGroupsFromUgi(user)
    }
  }

}
