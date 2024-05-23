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
package org.apache.kyuubi.grpc.client

import java.util.UUID

import io.grpc.ManagedChannel

import org.apache.kyuubi.grpc.proto.{GrpcTestServiceGrpc, TestAddRequest, TestAddResponse, TestOpenSessionRequest, TestOpenSessionResponse}

class SimpleRpcClient(val channel: ManagedChannel) {
  private val DEFAULT_USER_ID = "kyuubi_grpc_test"
  private val sessionId: String = UUID.randomUUID().toString
  private val stub = GrpcTestServiceGrpc.newBlockingStub(channel)

  def openSession(): TestOpenSessionResponse = {
    val request = TestOpenSessionRequest.newBuilder()
      .setUserId(DEFAULT_USER_ID)
      .setSessionId(sessionId)
      .build()
    stub.testOpenSession(request)
  }

  def testAdd(firstNum: Int, secondNum: Int): TestAddResponse = {
    val request = TestAddRequest.newBuilder()
      .setUserId(DEFAULT_USER_ID)
      .setSessionId(sessionId)
      .setFirstNum(firstNum)
      .setSecondNum(secondNum)
      .build()
    stub.testAdd(request)
  }

}
