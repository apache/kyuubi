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

import java.text.SimpleDateFormat

import com.fasterxml.jackson.databind.json.JsonMapper
import org.apache.ranger.admin.client.RangerAdminRESTClient
import org.apache.ranger.plugin.util.ServicePolicies

class RangerLocalClient extends RangerAdminRESTClient with RangerClientHelper {

  private val mapper = new JsonMapper()
    .setDateFormat(new SimpleDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z"))

  private val policies: ServicePolicies = {
    val loader = Thread.currentThread().getContextClassLoader
    val inputStream = {
      loader.getResourceAsStream("sparkSql_hive_jenkins.json")
    }
    mapper.readValue(inputStream, classOf[ServicePolicies])
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
}
