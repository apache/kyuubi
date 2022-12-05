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

import java.io.InputStreamReader
import java.util

import scala.collection.JavaConverters._

import com.google.gson.GsonBuilder
import org.apache.ranger.admin.client.RangerAdminRESTClient
import org.apache.ranger.plugin.util.{RangerUserStore, ServicePolicies}

class RangerLocalClient extends RangerAdminRESTClient with RangerClientHelper {

  private val g =
    new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create
  private val policies: ServicePolicies = {
    val loader = Thread.currentThread().getContextClassLoader
    val inputStream = {
      loader.getResourceAsStream("sparkSql_hive_jenkins.json")
    }
    g.fromJson(new InputStreamReader(inputStream), classOf[ServicePolicies])
  }

  override def getServicePoliciesIfUpdated(
      lastKnownVersion: Long,
      lastActivationTimeInMillis: Long): ServicePolicies = {
    policies
  }

  override def getServicePoliciesIfUpdated(
      lastKnownVersion: Long): ServicePolicies = {
    policies
  }

  override def getUserStoreIfUpdated(
      lastKnownUserStoreVersion: Long,
      lastActivationTimeInMillis: Long): RangerUserStore = {
    val userGroupsMapping = new util.HashMap[String, util.Set[String]]()
    userGroupsMapping.put("bob", Set("group_a", "group_b").asJava)
    val groupAttrMapping = new util.HashMap[String, util.Map[String, String]]()
    val userAttrMapping = new util.HashMap[String, util.Map[String, String]]()
    val userAttr1 = new util.HashMap[String, String]()
    userAttr1.put("city", "guangzhou")
    userAttrMapping.put("bob", userAttr1)

    val userStore = new RangerUserStore()
    userStore.setUserGroupMapping(userGroupsMapping)
    userStore.setGroupAttrMapping(groupAttrMapping)
    userStore.setUserAttrMapping(userAttrMapping)
    userStore
  }
}

/**
 * bypass scala lang restriction `overrides nothing`
 */
trait RangerClientHelper {

  /**
   * Apache ranger 0.7.x ~
   */
  def getServicePoliciesIfUpdated(
      lastKnownVersion: Long,
      lastActivationTimeInMillis: Long): ServicePolicies

  /**
   * Apache ranger 0.6.x
   */
  def getServicePoliciesIfUpdated(
      lastKnownVersion: Long): ServicePolicies

  /**
   * Apache ranger 2.1.0+
   */
  def getUserStoreIfUpdated(
      lastKnownUserStoreVersion: Long,
      lastActivationTimeInMillis: Long): RangerUserStore

}
