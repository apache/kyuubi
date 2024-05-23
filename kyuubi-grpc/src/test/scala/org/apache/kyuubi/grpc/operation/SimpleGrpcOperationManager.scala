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
package org.apache.kyuubi.grpc.operation

import io.grpc.stub.StreamObserver

import org.apache.kyuubi.grpc.proto.{TestAddRequest, TestAddResponse, TestOpenSessionRequest, TestOpenSessionResponse}
import org.apache.kyuubi.grpc.session.SimpleGrpcSessionImpl

class SimpleGrpcOperationManager
  extends GrpcOperationManager("SimpleGrpcOperationManager") {

  def newSimpleOpenSessionOperation(
      session: SimpleGrpcSessionImpl,
      shouldFail: Boolean,
      request: TestOpenSessionRequest,
      responseObserver: StreamObserver[TestOpenSessionResponse]): GrpcOperation = {
    val operation =
      new SimpleOpenSessionOperationImpl(session, shouldFail, request, responseObserver)
    addOperation(operation)
  }

  def newSimpleAddOperation(
      session: SimpleGrpcSessionImpl,
      shouldFail: Boolean,
      request: TestAddRequest,
      responseObserver: StreamObserver[TestAddResponse]): GrpcOperation = {
    val operation = new SimpleAddOperationImpl(session, shouldFail, request, responseObserver)
    addOperation(operation)
  }
}
