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
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader

class YarnAppPartitionReader(inputPartition: YarnAppPartition)
  extends PartitionReader[InternalRow] {

  private val appIterator = fetchApp(inputPartition).iterator

  override def next(): Boolean = appIterator.hasNext

  override def get(): InternalRow = {
    val app = appIterator.next()
    new GenericInternalRow(Array[Any](
      app.id,
      app.appType,
      app.user,
      app.name,
      app.state,
      app.queue,
      app.attemptId,
      app.submitTime,
      app.launchTime,
      app.startTime,
      app.finishTime))
  }

  override def close(): Unit = {}

  private def fetchApp(inputPartition: YarnAppPartition): Seq[YarnApplication] = {
    val yarnClient = YarnClient.createYarnClient()
    val yarnConf = new YarnConfiguration()
    yarnClient.init(yarnConf)
    yarnClient.start()
    // fet apps
    val applicationReports = yarnClient.getApplications()
    val appSeq = Seq[YarnApplication]()
    applicationReports.forEach(app => {
      appSeq :+ YarnApplication(
        id = app.getApplicationType,
        appType = app.getApplicationType,
        user = app.getUser,
        name = app.getName,
        state = app.getYarnApplicationState.name,
        queue = app.getQueue,
        attemptId = app.getCurrentApplicationAttemptId.toString,
        submitTime = app.getSubmitTime,
        launchTime = app.getLaunchTime,
        startTime = app.getStartTime,
        finishTime = app.getFinishTime)
    })
    yarnClient.close()
    appSeq
  }
}

// Helper class to represent app
case class YarnApplication(
    id: String,
    appType: String,
    user: String,
    name: String,
    state: String,
    queue: String,
    attemptId: String,
    submitTime: Long,
    launchTime: Long,
    startTime: Long,
    finishTime: Long)
