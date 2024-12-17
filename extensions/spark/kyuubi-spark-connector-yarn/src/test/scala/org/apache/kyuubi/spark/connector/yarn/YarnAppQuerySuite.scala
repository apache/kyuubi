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

import scala.jdk.CollectionConverters.asScalaBufferConverter

import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession

class YarnAppQuerySuite extends SparkYarnConnectorWithYarn {
  test("query apps") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.yarn", classOf[YarnCatalog].getName)
    miniHdfsService.getHadoopConf.forEach(kv => {
      if (kv.getKey.startsWith("fs")) {
        sparkConf.set(s"spark.hadoop.${kv.getKey}", kv.getValue)
      }
    })
    miniYarnService.getYarnConf.forEach(kv => {
      if (kv.getKey.startsWith("yarn")) {
        sparkConf.set(s"spark.hadoop.${kv.getKey}", kv.getValue)
      }
    })
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      spark.sql("USE yarn")
      val appCnt = spark.sql("select count(1) from yarn.default.apps")
        .collect().head.asInstanceOf[GenericRowWithSchema].getLong(0)
      assert(appCnt >= 0L)
    }
  }

  test("query app with appId") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.yarn", classOf[YarnCatalog].getName)
    miniHdfsService.getHadoopConf.forEach(kv => {
      if (kv.getKey.startsWith("fs")) {
        sparkConf.set(s"spark.hadoop.${kv.getKey}", kv.getValue)
      }
    })
    miniYarnService.getYarnConf.forEach(kv => {
      if (kv.getKey.startsWith("yarn")) {
        sparkConf.set(s"spark.hadoop.${kv.getKey}", kv.getValue)
      }
    })
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      spark.sql("USE yarn")
      val yarnClient = YarnClient.createYarnClient()
      yarnClient.init(yarnConf)
      yarnClient.start()
      yarnClient.getApplications.forEach(app => {
        val appId = app.getApplicationId.toString
        val queryCnt = spark.sql("select count(1) from yarn.default.apps " +
          s"where id = '${appId}'").collect().head.getLong(0)
        assert(queryCnt == 1)
        val queryApps = spark.sql(s"select * from yarn.default.apps " +
          s"where id = '${appId}'").collect()
          .map(x => x.asInstanceOf[GenericRowWithSchema])
          .map(x =>
            YarnApplication(
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
              finishTime = x.getLong(10),
              trackingUrl = x.getString(11),
              originalTrackingUrl = x.getString(12)))
        val queryApp = queryApps.head
        assert(queryApp.appType == app.getApplicationType)
        assert(queryApp.user == app.getUser)
        assert(queryApp.name == app.getName)
        assert(queryApp.state == app.getYarnApplicationState.name)
        assert(queryApp.queue == app.getQueue)
        assert(queryApp.attemptId == app.getCurrentApplicationAttemptId.toString)
        assert(queryApp.submitTime == app.getSubmitTime)
        assert(queryApp.launchTime == app.getLaunchTime)
        assert(queryApp.startTime == app.getStartTime)
        assert(queryApp.finishTime == app.getFinishTime)
        assert(queryApp.trackingUrl == app.getTrackingUrl)
        assert(queryApp.originalTrackingUrl == app.getOriginalTrackingUrl)
      })
      yarnClient.close()
    }
  }

  test("query app with equalTo appType") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.yarn", classOf[YarnCatalog].getName)
    miniHdfsService.getHadoopConf.forEach(kv => {
      if (kv.getKey.startsWith("fs")) {
        sparkConf.set(s"spark.hadoop.${kv.getKey}", kv.getValue)
      }
    })
    miniYarnService.getYarnConf.forEach(kv => {
      if (kv.getKey.startsWith("yarn")) {
        sparkConf.set(s"spark.hadoop.${kv.getKey}", kv.getValue)
      }
    })
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      spark.sql("USE yarn")
      val yarnClient = YarnClient.createYarnClient()
      yarnClient.init(yarnConf)
      yarnClient.start()
      yarnClient.getApplications().asScala.map(x => x.getYarnApplicationState.toString)
        .map(x => (x, 1))
        .groupBy(x => x._1)
        .mapValues(values => values.map(_._2).sum)
        .foreach(x => {
          val appState = x._1
          val appCnt = x._2
          val queryCnt = spark.sql("select count(1) from yarn.default.apps " +
            s"where state = '${appState}'").collect().head.getLong(0)
          assert(queryCnt == appCnt)
        })
      yarnClient.close()
    }
  }

}
