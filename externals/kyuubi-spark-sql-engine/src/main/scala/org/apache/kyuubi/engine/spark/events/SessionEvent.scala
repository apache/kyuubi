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

package org.apache.kyuubi.engine.spark.events

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.util.kvstore.KVIndex

import org.apache.kyuubi.Utils
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil.KVIndexParam
import org.apache.kyuubi.engine.spark.session.SparkSessionImpl
import org.apache.kyuubi.events.KyuubiEvent

/**
 * Event Tracking for user sessions
 * @param sessionId the identifier of a session
 * @param engineId the engine id
 * @param startTime Start time
 * @param endTime End time
 * @param ip Client IP address
 * @param serverIp Kyuubi Server IP address
 * @param totalOperations how many queries and meta calls
 * @param sessionRunTime total time of running the session (including fetching shuffle data)
 *                       in milliseconds
 * @param sessionCpuTime total CPU time of running the session (including fetching shuffle data)
 *                       in nanoseconds
 */
case class SessionEvent(
    @KVIndexParam sessionId: String,
    name: String,
    engineId: String,
    username: String,
    ip: String,
    serverIp: String,
    conf: Map[String, String],
    startTime: Long,
    var endTime: Long = -1L,
    var totalOperations: Int = 0,
    var sessionRunTime: Long = 0,
    var sessionCpuTime: Long = 0) extends KyuubiEvent with SparkListenerEvent {

  override lazy val partitions: Seq[(String, String)] =
    ("day", Utils.getDateFromTimestamp(startTime)) :: Nil

  def duration: Long = {
    if (endTime == -1L) {
      System.currentTimeMillis - startTime
    } else {
      endTime - startTime
    }
  }

  @JsonIgnore @KVIndex("endTime")
  private def endTimeIndex: Long = if (endTime > 0L) endTime else -1L
}

object SessionEvent {
  def apply(session: SparkSessionImpl): SessionEvent = {
    new SessionEvent(
      session.handle.identifier.toString,
      session.name.getOrElse(""),
      KyuubiSparkUtil.engineId,
      session.user,
      session.clientIpAddress,
      session.ipAddress,
      session.conf,
      session.createTime)
  }
}
