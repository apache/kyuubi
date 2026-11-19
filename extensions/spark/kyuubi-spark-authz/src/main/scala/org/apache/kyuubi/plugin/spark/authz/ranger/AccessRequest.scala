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

import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.plugin.spark.authz.OperationType.OperationType
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessType.AccessType

/**
 * A request to authorize a user for accessing a resource with an access type.
 *
 * @param resource   the resource to authorize
 * @param user       the name of the user to authorize
 * @param userGroups the groups of the user
 * @param opType     the Spark SQL operation type requesting the access
 * @param accessType the access type to authorize
 */
case class AccessRequest private[ranger] (
    resource: AccessResource,
    user: String,
    userGroups: Set[String],
    opType: OperationType,
    accessType: AccessType)

object AccessRequest {

  def apply(
      resource: AccessResource,
      user: UserGroupInformation,
      opType: OperationType,
      accessType: AccessType): AccessRequest = {
    AccessRequest(resource, user.getShortUserName, user.getGroupNames.toSet, opType, accessType)
  }
}
