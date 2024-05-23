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

import org.apache.kyuubi.grpc.proto.{TestAddRequest, TestAddResponse}
import org.apache.kyuubi.grpc.session.SimpleGrpcSessionImpl

class SimpleAddOperationImpl(
    grpcSession: SimpleGrpcSessionImpl,
    shouldFail: Boolean,
    request: TestAddRequest,
    responseObserver: StreamObserver[TestAddResponse])
  extends SimpleGrpcOperationImpl(grpcSession, shouldFail) {

  override protected def key: OperationKey = OperationKey(grpcSession.sessionKey)

  override def runInternal(): Unit = {
    super.runInternal()
    val result = request.getFirstNum + request.getSecondNum
    val builder = TestAddResponse.newBuilder()
      .setOperationId(operationKey.operationId)
      .setSessionId(operationKey.sessionId)
      .setServerSideSessionId(grpcSession.serverSessionId)
      .setResult(result)
    responseObserver.onNext(builder.build())
    responseObserver.onCompleted()
  }

}
