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
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.util.kvstore.KVIndex

import org.apache.kyuubi.Utils
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil.KVIndexParam
import org.apache.kyuubi.engine.spark.operation.SparkOperation
import org.apache.kyuubi.events.{ExceptionDeserializer, ExceptionSerializer, KyuubiEvent}

/**
 * A [[SparkOperationEvent]] used to tracker the lifecycle of an operation at Spark SQL Engine side.
 * <ul>
 *   <li>Operation Basis</li>
 *   <li>Operation Live Status</li>
 *   <li>Parent Session Id</li>
 * </ul>
 *
 * @param statementId the unique identifier of a single operation
 * @param statement the sql that you execute
 * @param shouldRunAsync the flag indicating whether the query runs synchronously or not
 * @param state the current operation state
 * @param eventTime the time when the event created & logged
 * @param createTime the time for changing to the current operation state
 * @param startTime the time the query start to time of this operation
 * @param completeTime time time the query ends
 * @param exception: caught exception if have
 * @param sessionId the identifier of the parent session
 * @param sessionUser the authenticated client user
 * @param executionId the query execution id of this operation
 * @param operationRunTime total time of running the operation (including fetching shuffle data)
 *                         in milliseconds
 * @param operationCpuTime total CPU time of running the operation (including fetching shuffle data)
 *                         in nanoseconds
 */
case class SparkOperationEvent(
    @KVIndexParam statementId: String,
    statement: String,
    shouldRunAsync: Boolean,
    state: String,
    eventTime: Long,
    createTime: Long,
    startTime: Long,
    completeTime: Long,
    @JsonSerialize(contentUsing = classOf[ExceptionSerializer])
    @JsonDeserialize(contentUsing = classOf[ExceptionDeserializer])
    exception: Option[Throwable],
    sessionId: String,
    sessionUser: String,
    executionId: Option[Long],
    operationRunTime: Option[Long],
    operationCpuTime: Option[Long]) extends KyuubiEvent with SparkListenerEvent {

  override def partitions: Seq[(String, String)] =
    ("day", Utils.getDateFromTimestamp(createTime)) :: Nil

  def duration: Long = {
    if (completeTime <= 0L) {
      System.currentTimeMillis - createTime
    } else {
      completeTime - createTime
    }
  }

  @JsonIgnore @KVIndex("completeTime")
  private def completeTimeIndex: Long = if (completeTime > 0L) completeTime else -1L
}

object SparkOperationEvent {
  def apply(
      operation: SparkOperation,
      executionId: Option[Long] = None,
      operationRunTime: Option[Long] = None,
      operationCpuTime: Option[Long] = None): SparkOperationEvent = {
    val session = operation.getSession
    val status = operation.getStatus
    new SparkOperationEvent(
      operation.statementId,
      operation.redactedStatement,
      operation.shouldRunAsync,
      status.state.name(),
      status.lastModified,
      status.create,
      status.start,
      status.completed,
      status.exception,
      session.handle.identifier.toString,
      session.user,
      executionId,
      operationRunTime,
      operationCpuTime)
  }
}
