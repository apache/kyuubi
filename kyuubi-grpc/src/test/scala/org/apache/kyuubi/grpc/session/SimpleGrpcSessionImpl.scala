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
package org.apache.kyuubi.grpc.session

import scala.util.Random

import io.grpc.stub.StreamObserver

import org.apache.kyuubi.grpc.event.SimpleSessionEventsManager
import org.apache.kyuubi.grpc.events.SessionEventsManager
import org.apache.kyuubi.grpc.operation.{GrpcOperation, OperationKey}
import org.apache.kyuubi.grpc.proto.{TestAddRequest, TestAddResponse, TestOpenSessionRequest, TestOpenSessionResponse}
import org.apache.kyuubi.grpc.utils.SystemClock

class SimpleGrpcSessionImpl(
    userId: String,
    sessionManager: SimpleGrpcSessionManager)
  extends AbstractGrpcSession(userId) {
  override def name: Option[String] = Some("SimpleGrpcSessionImpl")

  override def serverSessionId: String = Random.nextString(10)

  override def sessionEventsManager: SessionEventsManager =
    new SimpleSessionEventsManager(this, new SystemClock)

  def openSession(
      key: SessionKey,
      request: TestOpenSessionRequest,
      responseObserver: StreamObserver[TestOpenSessionResponse]): Unit = {
    val operation = sessionManager.grpcOperationManager
      .newSimpleOpenSessionOperation(this, false, request, responseObserver)
    runGrpcOperation(operation)
  }

  def add(
      key: SessionKey,
      request: TestAddRequest,
      responseObserver: StreamObserver[TestAddResponse]): Unit = {
    val operation = sessionManager.grpcOperationManager
      .newSimpleAddOperation(this, false, request, responseObserver)
    runGrpcOperation(operation)
  }

  override def getOperation(operationKey: OperationKey): GrpcOperation =
    sessionManager.grpcOperationManager.getOperation(operationKey)

  override def sessionManager: GrpcSessionManager[SimpleGrpcSessionImpl] = sessionManager

  override def close(): Unit = {
    sessionEventsManager.postClosed()
    super.close()
  }
}
