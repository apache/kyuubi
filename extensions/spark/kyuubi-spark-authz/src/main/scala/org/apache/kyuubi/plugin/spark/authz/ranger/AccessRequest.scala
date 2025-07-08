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
import org.apache.spark.internal.Logging

import org.apache.kyuubi.plugin.spark.authz.OperationType.OperationType
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessType._
import org.apache.kyuubi.util.reflect.DynMethods
import org.apache.kyuubi.util.reflect.DynMethods.UnboundMethod
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
      getRolesFromUserAndGroupsMethod.zip(setUserRolesMethod).foreach {
        case (getMethod, setMethod) =>
          val roles = getMethod.invoke[JSet[String]](SparkRangerAdminPlugin, userName, userGroups)
          setMethod.invoke[Unit](req, roles)
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

  private val getRolesFromUserAndGroupsMethod: Option[UnboundMethod] =
    getMethod(
      classOf[SparkRangerAdminPlugin],
      "getRolesFromUserAndGroups",
      classOf[String],
      classOf[JSet[String]])

  private val setUserRolesMethod: Option[UnboundMethod] =
    getMethod(classOf[AccessRequest], "setUserRoles", classOf[JSet[String]])

  private def getUserGroupsFromUgi(user: UserGroupInformation): JSet[String] = {
    user.getGroupNames.toSet.asJava
  }

  private lazy val getUserStoreEnricherMethod: Option[UnboundMethod] =
    getMethod(SparkRangerAdminPlugin.getClass, "getUserStoreEnricher")

  private lazy val getRangerUserStoreMethod: Option[UnboundMethod] =
    getMethod(
      Class.forName("org.apache.ranger.plugin.contextenricher.RangerUserStoreEnricher"),
      "getRangerUserStore")

  private lazy val getUserGroupMappingMethod: Option[UnboundMethod] =
    getMethod(
      Class.forName("org.apache.ranger.plugin.util.RangerUserStore"),
      "getUserGroupMapping")

  private def getUserGroupsFromUserStore(user: UserGroupInformation): Option[JSet[String]] = {
    try {
      getUserStoreEnricherMethod.zip(getRangerUserStoreMethod)
        .zip(getUserGroupMappingMethod).map {
          case ((getEnricher, getUserStore), getMapping) =>
            val enricher = getEnricher.invoke[AnyRef](SparkRangerAdminPlugin)
            val userStore = getUserStore.invoke[AnyRef](enricher)
            val userGroupMapping = getMapping.invoke[JHashMap[String, JSet[String]]](userStore)
            userGroupMapping.get(user.getShortUserName)
        }.headOption
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

  private def getMethod(
      clz: Class[_],
      methodName: String,
      argClasses: Class[_]*): Option[UnboundMethod] = {
    try {
      Some(DynMethods.builder(methodName)
        .hiddenImpl(clz, argClasses: _*)
        .impl(clz, argClasses: _*)
        .buildChecked)
    } catch {
      case e: Exception =>
        logWarning(
          s"$clz does not have $methodName${argClasses.map(_.getName).mkString("(", ", ", ")")}",
          e)
        None
    }
  }
}
