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

// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.ranger.SparkRangerAdminPlugin.{defaultAppId, getRangerPlugin, serviceType}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.isRanger21orGreater

class SparkRangerAdminPluginFactorySuite extends AnyFunSuite {
// scalastyle:on
  test("[KYUUBI #3594] get or create Ranger plugin by catalog name with Ranger 2.1+") {
    assume(isRanger21orGreater)

    val rangerPlugin1 = getRangerPlugin()
    assertResult((serviceType, "hive_jenkins", defaultAppId))((
      rangerPlugin1.getServiceType,
      rangerPlugin1.getServiceName,
      rangerPlugin1.getAppId))

    val catalog2 = "catalog2"
    val rangerPlugin2 = getRangerPlugin(Some(catalog2))
    assertResult((serviceType, s"hive_jenkins_$catalog2", catalog2))((
      rangerPlugin2.getServiceType,
      rangerPlugin2.getServiceName,
      rangerPlugin2.getAppId))
  }

  test("[KYUUBI #3594] same service name of Ranger plugin for catalogs with Ranger 2.0 and below") {
    assume(!isRanger21orGreater)

    val rangerPlugin1 = getRangerPlugin()
    assertResult((serviceType, "hive_jenkins", defaultAppId))((
      rangerPlugin1.getServiceType,
      rangerPlugin1.getServiceName,
      rangerPlugin1.getAppId))

    val catalog2 = "catalog2"
    val rangerPlugin2 = getRangerPlugin(Some(catalog2))
    assertResult((serviceType, "hive_jenkins", catalog2))((
      rangerPlugin2.getServiceType,
      rangerPlugin2.getServiceName,
      rangerPlugin2.getAppId))
  }
}
