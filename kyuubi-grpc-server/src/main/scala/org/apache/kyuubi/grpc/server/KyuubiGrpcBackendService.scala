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

import io.grpc.ManagedChannel
import io.grpc.stub.StreamObserver

import org.apache.kyuubi.grpc.service.AbstractGrpcBackendService
import org.apache.kyuubi.grpc.session.{GrpcSessionManager, KyuubiGrpcSession, KyuubiGrpcSessionManager, SessionKey}
import org.apache.kyuubi.shade.org.apache.spark.connect.proto._

class KyuubiGrpcBackendService(name: String) extends AbstractGrpcBackendService(name) {

  def config(
      sessionKey: SessionKey,
      request: ConfigRequest,
      responseObserver: StreamObserver[ConfigResponse],
      channel: ManagedChannel): Unit = {
    grpcSessionManager.openSession(sessionKey)
      .config(request, responseObserver, channel)
  }

  def this() = this(classOf[KyuubiGrpcBackendService].getSimpleName)

  override def grpcSessionManager: GrpcSessionManager[KyuubiGrpcSession] =
    new KyuubiGrpcSessionManager()
}
