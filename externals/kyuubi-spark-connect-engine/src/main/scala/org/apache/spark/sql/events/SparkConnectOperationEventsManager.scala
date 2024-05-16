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
package org.apache.spark.sql.events

import com.fasterxml.jackson.annotation.JsonIgnore
import com.google.protobuf.Message
import org.apache.spark.connect.proto
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.kyuubi.engine.spark.connect.operation.SparkConnectOperation
import org.apache.kyuubi.grpc.operation.OperationEventsManager
import org.apache.kyuubi.grpc.utils.Clock

class SparkConnectOperationEventsManager(
    operation: SparkConnectOperation,
    request: Option[proto.ExecutePlanRequest] = None,
    clock: Clock) extends OperationEventsManager(operation, clock) {

  override def postStarted(): Unit = {
    super.postStarted()
    if (request.isDefined) {
      val executePlanRequest = request.get
      val plan: Message = {
        executePlanRequest.getPlan.getOpTypeCase match {
          case proto.Plan.OpTypeCase.COMMAND => executePlanRequest.getPlan.getCommand
          case proto.Plan.OpTypeCase.ROOT => executePlanRequest.getPlan.getRoot
          case _ =>
            throw new UnsupportedOperationException(
              s"${executePlanRequest.getPlan.getOpTypeCase} not supported."
            )
        }
      }
      val event = SparkListenerConnectOperationStarted(
        operation.operationTag,
        operation.operationKey.operationId,
        clock.getTimeMillis(),
        executePlanRequest.getSessionId,
        executePlanRequest.getUserContext.getUserId,
        executePlanRequest.getUserContext.getUserName,
        Utils
      )
    }
  }
}

case class SparkListenerConnectOperationStarted(
    tag: String,
    operationId: String,
    eventTime: Long,
    sessionId: String,
    userId: String,
    userName: String,
    statementText: String,
    sparkSessionTags: Set[String],
    extraTags: Map[String, String] = Map.empty) extends SparkListenerEvent {
  @JsonIgnore val planRequest: Option[proto.ExecutePlanRequest] = None
}

case class SparkListenerConnectOperationAnalyzed(
    tag: String,
    operationId: String,
    eventTime: Long,
    extraTags: Map[String, String] = Map.empty) extends SparkListenerEvent {
  @JsonIgnore val analyzedPlan: Option[LogicalPlan] = None
}

case class SparkListenerConnectOperationReadyForExecution(
    tag: String,
    operationId: String,
    eventTime: Long,
    extraTags: Map[String, String] = Map.empty) extends SparkListenerEvent

case class SparkListenerConnectOperationCanceled(
    tag: String,
    operationId: String,
    eventTime: Long,
    extraTags: Map[String, String] = Map.empty) extends SparkListenerEvent

case class SparkListenerConnectOperationFailed(
    tag: String,
    operationId: String,
    eventTime: Long,
    errorMessage: String,
    extraTags: Map[String, String] = Map.empty) extends SparkListenerEvent

case class SparkListenerConnectOperationFinished(
    tag: String,
    operationId: String,
    eventTime: Long,
    producedRowCount: Option[Long] = None,
    extraTags: Map[String, String] = Map.empty) extends SparkListenerEvent

case class SparkListenerConnectOperationClosed(
    tag: String,
    operationId: String,
    eventTime: Long,
    extraTags: Map[String, String] = Map.empty) extends SparkListenerEvent
