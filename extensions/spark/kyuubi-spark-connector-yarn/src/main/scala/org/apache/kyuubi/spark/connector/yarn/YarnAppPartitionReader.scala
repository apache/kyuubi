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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.jdk.CollectionConverters.asScalaBufferConverter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationReport, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources.{EqualTo, In}
import org.apache.spark.unsafe.types.UTF8String

class YarnAppPartitionReader(yarnAppPartition: YarnAppPartition)
  extends PartitionReader[InternalRow] with Logging {

  private val validYarnStateSet =
    Set("NEW", "NEW_SAVING", "SUBMITTED", "ACCEPTED", "RUNNING", "FINISHED", "FAILED", "KILLED")

  private val appIterator = fetchApp().iterator

  override def next(): Boolean = appIterator.hasNext

  override def get(): InternalRow = {
    val app = appIterator.next()
    new GenericInternalRow(Array[Any](
      UTF8String.fromString(app.id),
      UTF8String.fromString(app.appType),
      UTF8String.fromString(app.user),
      UTF8String.fromString(app.name),
      UTF8String.fromString(app.state),
      UTF8String.fromString(app.queue),
      UTF8String.fromString(app.attemptId),
      app.submitTime,
      app.launchTime,
      app.startTime,
      app.finishTime,
      UTF8String.fromString(app.trackingUrl),
      UTF8String.fromString(app.originalTrackingUrl)))
  }

  override def close(): Unit = {}

  private def fetchApp(): mutable.Seq[YarnApplication] = {
    val hadoopConf = new Configuration()
    yarnAppPartition.hadoopConfMap.foreach(kv => hadoopConf.set(kv._1, kv._2))
    val yarnClient = YarnClient.createYarnClient()
    yarnClient.init(hadoopConf)
    yarnClient.start()
    // fet apps
    val applicationReports: java.util.List[ApplicationReport] =
      yarnAppPartition.filters match {
        case filters if filters.isEmpty => yarnClient.getApplications
        // id => point query
        // state => batch query
        // type => in (a,b,c), batch query
        case filters =>
          filters.collectFirst {
            case EqualTo("id", appId: String) => java.util.Collections.singletonList(
                yarnClient.getApplicationReport(ApplicationId.fromString(appId)))
            case EqualTo("state", state: String) =>
              state.toUpperCase match {
                case validState if validYarnStateSet.contains(validState) =>
                  yarnClient.getApplications(
                    java.util.EnumSet.of(YarnApplicationState.valueOf(validState)))
                case _ => Seq.empty[ApplicationReport].asJava
              }
            case EqualTo("type", appType: String) =>
              yarnClient.getApplications(java.util.Collections.singleton(appType))
            case In("state", states: Array[Any]) => yarnClient.getApplications(
                java.util.EnumSet.copyOf(states
                  .map(x => x.toString.toUpperCase)
                  .filter(x => validYarnStateSet.contains(x))
                  .map(x =>
                    YarnApplicationState.valueOf(x)).toList.asJava))
            case In("type", types: Array[Any]) => yarnClient.getApplications(
                types.map(x => x.toString).toSet.asJava)
            case _ => yarnClient.getApplications()
          }.get
      }

    val appSeq = applicationReports.asScala.map(app => {
      YarnApplication(
        id = app.getApplicationId.toString,
        appType = app.getApplicationType,
        user = app.getUser,
        name = app.getName,
        state = app.getYarnApplicationState.name,
        queue = app.getQueue,
        attemptId = app.getCurrentApplicationAttemptId.toString,
        submitTime = app.getSubmitTime,
        launchTime = app.getLaunchTime,
        startTime = app.getStartTime,
        finishTime = app.getFinishTime,
        trackingUrl = app.getTrackingUrl,
        originalTrackingUrl = app.getOriginalTrackingUrl)
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
    finishTime: Long,
    trackingUrl: String,
    originalTrackingUrl: String)
