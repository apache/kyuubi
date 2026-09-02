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

package org.apache.kyuubi.service

import java.net.SocketAddress

import io.grpc._

/**
 * Captures the remote address of the inbound gRPC connection into the call
 * context, so handlers can resolve the connection-scoped session without
 * touching the transport attributes directly.
 */
object GrpcRemoteAddressInterceptor {

  val REMOTE_ADDRESS: Context.Key[SocketAddress] = Context.key("kyuubi.grpc.remote.address")

  val INSTANCE: ServerInterceptor = new ServerInterceptor {
    override def interceptCall[ReqT, RespT](
        call: ServerCall[ReqT, RespT],
        headers: Metadata,
        next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
      val context = Context.current()
        .withValue(REMOTE_ADDRESS, call.getAttributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR))
      Contexts.interceptCall(context, call, headers, next)
    }
  }

  def get(): SocketAddress = REMOTE_ADDRESS.get()
}
