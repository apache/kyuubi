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
package org.apache.kyuubi.grpc.service

import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

import com.google.protobuf.MessageLite
import io.grpc.{ServerMethodDefinition, ServerServiceDefinition}
import io.grpc.stub.StreamObserver

import org.apache.kyuubi.grpc.proto.{GrpcTestServiceGrpc, TestAddRequest, TestAddResponse, TestOpenSessionRequest, TestOpenSessionResponse}
import org.apache.kyuubi.grpc.session.SessionKey
import org.apache.kyuubi.service.Service

class SimpleGrpcFrontendService(grpcSeverable: SimpleGrpcSeverable)
  extends AbstractGrpcFrontendService("SimpleGrpcFrontendService")
  with GrpcTestServiceGrpc.AsyncService {

  private def grpcBe = be.asInstanceOf[SimpleGrpcBackendService]
  override protected def serverHost: Option[String] = Some("localhost")

  override def bindService(): ServerServiceDefinition = {
    val serviceDef = GrpcTestServiceGrpc.bindService(this)
    val builder = ServerServiceDefinition.builder(serviceDef.getServiceDescriptor.getName)
    serviceDef.getMethods.asScala
      .asInstanceOf[Iterable[ServerMethodDefinition[MessageLite, MessageLite]]]
      .foreach(method =>
        builder.addMethod(
          methodWithCustomMarshallers(method.getMethodDescriptor),
          method.getServerCallHandler))
    builder.build()
  }

  override def testAdd(
      request: TestAddRequest,
      responseObserver: StreamObserver[TestAddResponse]): Unit = {
    val key = new SessionKey(request.getUserId, request.getSessionId)
    grpcBe.add(key, request, responseObserver)
  }

  override def testOpenSession(
      request: TestOpenSessionRequest,
      responseObserver: StreamObserver[TestOpenSessionResponse]): Unit = {
    val key = new SessionKey(request.getUserId, request.getSessionId)
    grpcBe.openSessionTesr(key, request, responseObserver)
  }

  override val serverable: SimpleGrpcSeverable = grpcSeverable
  override val discoveryService: Option[Service] = None
}
