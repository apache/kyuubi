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

package org.apache.kyuubi.server

import io.grpc.{Grpc, ServerCall, Status}

import org.apache.kyuubi.service.{GrpcProxyServerCallHandler, ResolvedProxyCall}
import org.apache.kyuubi.session.KyuubiGrpcSessionManager

/**
 * Byte-level passthrough for RPCs of unknown services, e.g. methods introduced
 * by extension plugins. Such RPCs carry no session key, so they are routed to
 * the session previously resolved on the same connection by the typed
 * Spark Connect proxy.
 */
class KyuubiGrpcProxyHandler(sessionManager: KyuubiGrpcSessionManager)
  extends GrpcProxyServerCallHandler {

  override def resolve(
      serverCall: ServerCall[Array[Byte], Array[Byte]],
      firstMessage: Array[Byte]): ResolvedProxyCall = {
    val session = sessionManager.getConnectionSession(
      serverCall.getAttributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR))
    if (session == null) {
      throw Status.UNIMPLEMENTED
        .withDescription(
          s"Unable to resolve the engine for " +
            s"${serverCall.getMethodDescriptor.getFullMethodName} " +
            "because the connection has no established Spark Connect session")
        .asRuntimeException()
    }
    ResolvedProxyCall(session.client.channel)
  }
}
