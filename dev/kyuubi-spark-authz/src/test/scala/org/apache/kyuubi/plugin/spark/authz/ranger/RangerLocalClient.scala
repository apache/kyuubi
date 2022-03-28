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

import com.google.gson.GsonBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.ranger.admin.client.RangerAdminRESTClient
import org.apache.ranger.plugin.util.ServicePolicies

class RangerLocalClient extends RangerAdminRESTClient {

  private var policies: ServicePolicies = _
  private val g =
    new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create

  override def init(
      serviceName: String,
      appId: String,
      propertyPrefix: String,
      config: Configuration): Unit = {
    val loader = Thread.currentThread().getContextClassLoader
    val inputStream = {
      loader.getResourceAsStream("sparkSql_hive_jenkins.json")
    }
    policies = g.fromJson(new InputStreamReader(inputStream), classOf[ServicePolicies])
  }

  def init(
      serviceName: String,
      appId: String,
      propertyPrefix: String): Unit = {
    val loader = Thread.currentThread().getContextClassLoader
    val inputStream = {
      loader.getResourceAsStream("sparkSql_hive_jenkins.json")
    }
    policies = g.fromJson(new InputStreamReader(inputStream), classOf[ServicePolicies])
  }

  override def getServicePoliciesIfUpdated(
      lastKnownVersion: Long,
      lastActivationTimeInMillis: Long): ServicePolicies = {
    policies
  }
}
