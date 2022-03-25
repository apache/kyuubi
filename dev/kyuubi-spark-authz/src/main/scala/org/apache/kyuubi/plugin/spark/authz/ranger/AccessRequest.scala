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

import java.util.{Set => JSet}
import java.util.Date

import org.apache.ranger.plugin.policyengine.{RangerAccessRequestImpl, RangerPolicyEngine}

import org.apache.kyuubi.plugin.spark.authz.ranger.AccessType._

case class AccessRequest(accessType: AccessType) extends RangerAccessRequestImpl

object AccessRequest {
  def apply(
      resource: AccessResource,
      user: String,
      groups: JSet[String],
      opType: String,
      accessType: AccessType): AccessRequest = {
    val req = new AccessRequest(accessType)
    req.setResource(resource)
    req.setUser(user)
    req.setUserGroups(groups)
    req.setUserRoles(RangerSparkPlugin.getRolesFromUserAndGroups(user, groups))
    req.setAccessTime(new Date())
    accessType match {
      case USE => req.setAccessType(RangerPolicyEngine.ANY_ACCESS)
      case _ => req.setAccessType(accessType.toString.toLowerCase)
    }
    req.setClusterName(RangerSparkPlugin.getClusterName)
    req
  }
}
