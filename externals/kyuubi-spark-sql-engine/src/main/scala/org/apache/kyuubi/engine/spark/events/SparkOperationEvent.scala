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

import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.Utils
import org.apache.kyuubi.engine.spark.operation.SparkOperation

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
 * @param queryExecution the query execution of this operation
 */
case class SparkOperationEvent(
    statementId: String,
    statement: String,
    shouldRunAsync: Boolean,
    state: String,
    eventTime: Long,
    createTime: Long,
    startTime: Long,
    completeTime: Long,
    exception: Option[Throwable],
    sessionId: String,
    sessionUser: String,
    queryExecution: String) extends KyuubiSparkEvent {

  override def schema: StructType = Encoders.product[SparkOperationEvent].schema
  override def partitions: Seq[(String, String)] =
    ("day", Utils.getDateFromTimestamp(createTime)) :: Nil

  def duration: Long = {
    if (completeTime == -1L) {
      System.currentTimeMillis - createTime
    } else {
      completeTime - createTime
    }
  }
}

object SparkOperationEvent {
  def apply(operation: SparkOperation, result: Option[DataFrame] = None): SparkOperationEvent = {
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
      result.map(_.queryExecution.toString).orNull)
  }
}
