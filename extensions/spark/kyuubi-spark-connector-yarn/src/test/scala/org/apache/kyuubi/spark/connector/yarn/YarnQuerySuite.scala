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

import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.jdk.CollectionConverters.asScalaBufferConverter

class YarnQuerySuite extends SparkYarnConnectorWithYarn {
  test("get app tables") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.yarn", classOf[YarnCatalog].getName)
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      spark.sql("USE yarn")
//      val appCnt = spark.sql("select count(1) from yarn.default.apps")
//        .collect().head.asInstanceOf[GenericRowWithSchema].getLong(0)
//      assert(appCnt >= 0L)
      val yarnApplications = spark.sql("select * from yarn.default.apps")
        .collect().map(x => x.asInstanceOf[GenericRowWithSchema])
        .map(x => YarnApplication(
          id = x.getString(0),
          appType = x.getString(1),
          user = x.getString(2),
          name = x.getString(3),
          state = x.getString(4),
          queue = x.getString(5),
          attemptId = x.getString(6),
          submitTime = x.getLong(7),
          launchTime = x.getLong(8),
          startTime = x.getLong(9),
          finishTime = x.getLong(10)
        ))
      val yarnClient = YarnClient.createYarnClient()
      yarnClient.init(yarnConf)
      yarnClient.start()
      yarnClient.getApplications.forEach(
        app => {
          val appId = app.getApplicationId.toString
          val queryApp = spark.sql(s"select * from yarn.default.apps " +
            s"where id = '${appId}'").collect()
          val searchedApps = yarnApplications.filter(x => x.id eq appId)
          assert(!searchedApps.isEmpty)
          val searchedApp = searchedApps.head
          assert(searchedApp.appType == app.getApplicationType)
          assert(searchedApp.user == app.getUser)
          assert(searchedApp.name == app.getName)
          assert(searchedApp.state == app.getYarnApplicationState.name)
          assert(searchedApp.queue == app.getQueue)
          assert(searchedApp.attemptId == app.getCurrentApplicationAttemptId.toString)
          assert(searchedApp.submitTime == app.getSubmitTime)
          assert(searchedApp.launchTime == app.getLaunchTime)
          assert(searchedApp.startTime == app.getStartTime)
          assert(searchedApp.finishTime == app.getFinishTime)
        }
      )
      yarnClient.close()
    }
  }
}
