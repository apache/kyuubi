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

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.gson.GsonBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.ranger.admin.client.RangerAdminRESTClient
import org.apache.ranger.plugin.util.ServicePolicies

class RangerLocalClient extends RangerAdminRESTClient with RangerClientHelper {
  private val g =
    new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create

  private var rangerPluginAppId: String = _

  private val policyLoader: LoadingCache[String, ServicePolicies] =
    CacheBuilder.newBuilder.maximumSize(3).build(new CacheLoader[String, ServicePolicies]() {
      override def load(key: String): ServicePolicies = {
        val loader = Thread.currentThread().getContextClassLoader
        val inputStream = {
          val fileName = rangerPluginAppId match {
            case "catalog2" => "sparkSql_hive_jenkins_catalog2.json"
            case _ => "sparkSql_hive_jenkins.json"
          }
          loader.getResourceAsStream(fileName)
        }
        val a = g.fromJson(new InputStreamReader(inputStream), classOf[ServicePolicies])
        a
      }
    })

  private def policies: ServicePolicies = {
    val jsonFileName: String = rangerPluginAppId match {
      case "catalog2" => "sparkSql_hive_jenkins_catalog2.json"
      case _ => "sparkSql_hive_jenkins.json"
    }
    policyLoader.get(jsonFileName)
  }

  override def init(
      serviceName: String,
      appId: String,
      configPropertyPrefix: String,
      config: Configuration): Unit = {
    this.rangerPluginAppId = appId
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
