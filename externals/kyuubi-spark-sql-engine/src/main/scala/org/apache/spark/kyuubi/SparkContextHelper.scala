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

package org.apache.spark.kyuubi

import org.apache.hadoop.security.Credentials
import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.scheduler.SchedulerBackend
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.UpdateDelegationTokens
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.local.LocalSchedulerBackend

import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.spark.events.KyuubiSparkEvent
import org.apache.kyuubi.events.EventLogger

/**
 * A place to invoke non-public APIs of [[SparkContext]], anything to be added here need to
 * think twice
 */
object SparkContextHelper extends Logging {

  def createSparkHistoryLogger(sc: SparkContext): EventLogger[KyuubiSparkEvent] = {
    new SparkHistoryEventLogger(sc)
  }

  def updateDelegationTokens(sc: SparkContext, creds: Credentials): Unit = {
    val bytes = SparkHadoopUtil.get.serialize(creds)
    sc.schedulerBackend match {
      case _: LocalSchedulerBackend =>
        SparkHadoopUtil.get.addDelegationTokens(bytes, sc.conf)
      case backend: CoarseGrainedSchedulerBackend =>
        backend.driverEndpoint.send(UpdateDelegationTokens(bytes))
      case backend: SchedulerBackend =>
        warn(s"Failed to update delegation tokens due to unsupported SchedulerBackend " +
          s"${backend.getClass.getName}.")
    }
  }

}

/**
 * A [[EventLogger]] that logs everything to SparkHistory
 * @param sc SparkContext
 */
private class SparkHistoryEventLogger(sc: SparkContext) extends EventLogger[KyuubiSparkEvent] {
  override def logEvent(kyuubiEvent: KyuubiSparkEvent): Unit = {
    sc.listenerBus.post(kyuubiEvent)
  }
}
