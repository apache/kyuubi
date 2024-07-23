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

package org.apache.kyuubi.grpc.server

import scala.jdk.CollectionConverters._

import com.google.protobuf.MessageLite
import io.grpc._
import io.grpc.stub.StreamObserver

import org.apache.kyuubi.grpc.session.SessionKey
import org.apache.kyuubi.service.Service
import org.apache.kyuubi.shade.org.apache.spark.connect.proto.{ConfigRequest, ConfigResponse, SparkConnectServiceGrpc}

abstract class KyuubiSparkConnectFrontendService(grpcSeverable: KyuubiGrpcSeverable, name: String)
  extends AbstractKyuubiGrpcFrontendService(grpcSeverable, name)
  with SparkConnectServiceGrpc.AsyncService {

  override def channel: ManagedChannel = {
    Grpc.newChannelBuilderForAddress(
      host,
      port,
      InsecureChannelCredentials.create()).build()
  }

  override protected def serverHost: Option[String] = Some("localhost")

  override def bindService(): ServerServiceDefinition = {
    val serviceDef = SparkConnectServiceGrpc.bindService(this)
    val builder = ServerServiceDefinition.builder(serviceDef.getServiceDescriptor.getName)
    serviceDef.getMethods.asScala
      .asInstanceOf[Iterable[ServerMethodDefinition[MessageLite, MessageLite]]]
      .foreach(method =>
        builder.addMethod(
          methodWithCustomMarshallers(method.getMethodDescriptor),
          method.getServerCallHandler))
    builder.build()
  }

  override val discoveryService: Option[Service] = None

  override def config(
      request: ConfigRequest,
      responseObserver: StreamObserver[ConfigResponse]): Unit = {
    val sessionKey = new SessionKey(request.getUserContext.getUserId, request.getSessionId)
    grpcBe.config(sessionKey, request, responseObserver, channel)
  }

}
