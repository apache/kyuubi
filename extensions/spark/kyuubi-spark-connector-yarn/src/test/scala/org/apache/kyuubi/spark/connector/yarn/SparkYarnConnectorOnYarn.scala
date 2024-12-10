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

package org.apache.kyuubi.spark.connector.yarn

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.client.api.YarnClient

class SparkYarnConnectorOnYarn extends WithKyuubiServerAndYarnMiniCluster {
  override def beforeAll(): Unit = {
    super.beforeAll()
    // init log dir and set permission
    val fs = FileSystem.get(hdfsConf)
    val logDir = new Path("/tmp/logs")
    fs.mkdirs(logDir)
    fs.setPermission(logDir, new org.apache.hadoop.fs.permission.FsPermission("777"))
    info(s"hdfs web address: http://${fs.getConf.get("dfs.http.address")}")
    fs.close()
    // mock app submit
    for (i <- 1 to 10) {
      submitMockTaskOnYarn()
    }
  }

  test("yarn - list applications") {
    val yarnClient = YarnClient.createYarnClient()
    yarnClient.init(yarnConf)
    yarnClient.start()
    val applications = yarnClient.getApplications
    applications.forEach(app => {
      val applicationId = app.getApplicationId
      val applicationType = app.getApplicationType
      val applicationName = app.getName
      val applicationState = app.getYarnApplicationState
      val currentApplicationAttemptId = app.getCurrentApplicationAttemptId
      val startTime = app.getStartTime
      val finishTime = app.getFinishTime
      val submitTime = app.getSubmitTime
      val launchTime = app.getLaunchTime
      info(s"get application info from client, ${applicationId}, " +
        s"submit: ${submitTime}" +
        s"launch: ${launchTime}" +
        s"start: ${startTime}" +
        s"finish: ${finishTime}" +
        s"${applicationName}, ${applicationType}, ${applicationState.name()}")
    })
    yarnClient.close()
  }
}
