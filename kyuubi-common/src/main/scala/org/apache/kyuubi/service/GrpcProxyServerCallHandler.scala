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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.util.concurrent.atomic.AtomicBoolean

import io.grpc._

import org.apache.kyuubi.Logging

object GrpcProxyServerCallHandler {

  val byteMarshaller: MethodDescriptor.Marshaller[Array[Byte]] =
    new MethodDescriptor.Marshaller[Array[Byte]] {
      override def parse(stream: InputStream): Array[Byte] = {
        val buffer = new ByteArrayOutputStream()
        val chunk = new Array[Byte](8192)
        var read = stream.read(chunk)
        while (read >= 0) {
          buffer.write(chunk, 0, read)
          read = stream.read(chunk)
        }
        buffer.toByteArray
      }

      override def stream(value: Array[Byte]): InputStream = new ByteArrayInputStream(value)
    }

  class PassthroughHandlerRegistry(handler: ServerCallHandler[Array[Byte], Array[Byte]])
    extends HandlerRegistry {

    override def lookupMethod(
        methodName: String,
        authority: String): ServerMethodDefinition[_, _] = {
      val methodDescriptor = MethodDescriptor.newBuilder(byteMarshaller, byteMarshaller)
        .setFullMethodName(methodName)
        .setType(MethodDescriptor.MethodType.UNKNOWN)
        .build()
      ServerMethodDefinition.create(methodDescriptor, handler)
    }
  }
}

/**
 * Per-call hooks that allow inspecting and rewriting the proxied traffic.
 *
 * Messages are exposed as raw bytes. Implementations that know the message
 * schema may decode and re-encode them; unknown messages can simply be
 * forwarded unchanged by returning [[GrpcCallProxyInterceptor.NOOP]].
 */
trait GrpcCallProxyInterceptor {

  def transformRequestHeaders(headers: Metadata): Metadata = headers

  def transformRequest(message: Array[Byte]): Array[Byte] = message

  def transformResponse(message: Array[Byte]): Array[Byte] = message

  def onRequestHalfClose(): Unit = {}

  def onResponseClose(status: Status, trailers: Metadata): Unit = {}
}

object GrpcCallProxyInterceptor {
  val NOOP: GrpcCallProxyInterceptor = new GrpcCallProxyInterceptor {}
}

/**
 * The backend target of a single server call.
 */
case class ResolvedProxyCall(
    channel: Channel,
    interceptor: GrpcCallProxyInterceptor = GrpcCallProxyInterceptor.NOOP)

/**
 * A generic gRPC-level proxy for a single server call.
 *
 * It forwards raw message bytes, so unary, server-streaming, client-streaming
 * and bidirectional-streaming calls, as well as RPCs of unknown services, e.g.
 * methods introduced by extension plugins, share the same code path. Message
 * traffic can be inspected or rewritten per call via [[GrpcCallProxyInterceptor]].
 *
 * The backend is resolved lazily on the first request message, because the
 * routing information, e.g. session id, is usually carried in the request
 * payload and the backend may be launched on demand.
 */
abstract class GrpcProxyServerCallHandler
  extends ServerCallHandler[Array[Byte], Array[Byte]] with Logging {

  /**
   * Resolves the backend target for the given call using its first request message.
   * This method may block until the backend is ready, and may throw a
   * [[StatusRuntimeException]] to fail the server call.
   */
  def resolve(serverCall: ServerCall[Array[Byte], Array[Byte]], firstMessage: Array[Byte])
      : ResolvedProxyCall

  /**
   * Invoked after the backend call is closed. Subclasses may use it to release
   * per-call resources, e.g. close the server-side session after `ReleaseSession`.
   */
  def onCallComplete(methodName: String, channel: Channel, status: Status): Unit = {}

  override def startCall(
      serverCall: ServerCall[Array[Byte], Array[Byte]],
      headers: Metadata): ServerCall.Listener[Array[Byte]] = {
    new PendingCall(serverCall, headers)
  }

  private class PendingCall(
      val serverCall: ServerCall[Array[Byte], Array[Byte]],
      headers: Metadata) extends ServerCall.Listener[Array[Byte]] {

    private val cancelled = new AtomicBoolean(false)
    private var callProxy: CallProxy = _
    serverCall.request(1)

    private def resolveTarget(firstMessage: Array[Byte]): Unit = synchronized {
      if (callProxy != null || cancelled.get) {
        return
      }
      val target =
        try {
          resolve(serverCall, firstMessage)
        } catch {
          case e: StatusRuntimeException =>
            serverCall.close(
              e.getStatus,
              if (e.getTrailers != null) e.getTrailers else new Metadata())
            return
          case e: Throwable =>
            serverCall.close(
              Status.INTERNAL.withDescription(String.valueOf(e.getMessage)),
              new Metadata())
            return
        }
      if (serverCall.isCancelled) {
        return
      }
      callProxy = new CallProxy(
        serverCall,
        target.channel.newCall(serverCall.getMethodDescriptor, CallOptions.DEFAULT),
        target.interceptor.transformRequestHeaders(headers),
        target.interceptor,
        target.channel)
    }

    override def onMessage(message: Array[Byte]): Unit = {
      if (cancelled.get) {
        return
      }
      resolveTarget(message)
      if (callProxy != null) {
        callProxy.serverCallListener.onMessage(message)
      }
    }

    override def onHalfClose(): Unit = {
      if (callProxy == null) {
        serverCall.close(
          Status.UNIMPLEMENTED.withDescription(
            "No request message received, unable to resolve the backend channel"),
          new Metadata())
      } else {
        callProxy.serverCallListener.onHalfClose()
      }
    }

    override def onCancel(): Unit = {
      cancelled.set(true)
      if (callProxy != null) {
        callProxy.serverCallListener.onCancel()
      }
    }

    override def onReady(): Unit = {
      if (callProxy != null) {
        callProxy.clientCallListener.onServerReady()
      }
    }
  }

  /**
   * Bridges the incoming server call and the outgoing client call, adapted from the
   * official grpc-java `grpcproxy` example.
   */
  private class CallProxy(
      val serverCall: ServerCall[Array[Byte], Array[Byte]],
      val clientCall: ClientCall[Array[Byte], Array[Byte]],
      headers: Metadata,
      interceptor: GrpcCallProxyInterceptor,
      channel: Channel) {

    private var serverCallClosed = false

    def closeServerCall(status: Status, trailers: Metadata): Unit = synchronized {
      if (!serverCallClosed) {
        serverCallClosed = true
        serverCall.close(status, trailers)
      }
    }

    val serverCallListener = new RequestProxy(clientCall, interceptor)
    val clientCallListener = new ResponseProxy(serverCall, interceptor)

    clientCall.start(clientCallListener, headers)
    clientCall.request(1)

    class RequestProxy(
        clientCall: ClientCall[Array[Byte], Array[Byte]],
        interceptor: GrpcCallProxyInterceptor)
      extends ServerCall.Listener[Array[Byte]] {

      private var needToRequest = false

      override def onCancel(): Unit = clientCall.cancel("Server cancelled", null)

      override def onHalfClose(): Unit = {
        interceptor.onRequestHalfClose()
        clientCall.halfClose()
      }

      override def onMessage(message: Array[Byte]): Unit = {
        val transformed =
          try {
            interceptor.transformRequest(message)
          } catch {
            case e: Throwable =>
              closeServerCall(
                Status.INTERNAL.withDescription(
                  s"Failed to transform the request message: ${e.getMessage}"),
                new Metadata())
              clientCall.cancel("Failed to transform the request message", e)
              return
          }
        clientCall.sendMessage(transformed)
        this.synchronized {
          if (clientCall.isReady) {
            serverCall.request(1)
          } else {
            needToRequest = true
          }
        }
      }

      override def onReady(): Unit = clientCallListener.onServerReady()

      def onClientReady(): Unit = this.synchronized {
        if (needToRequest) {
          serverCall.request(1)
          needToRequest = false
        }
      }
    }

    class ResponseProxy(
        serverCall: ServerCall[Array[Byte], Array[Byte]],
        interceptor: GrpcCallProxyInterceptor)
      extends ClientCall.Listener[Array[Byte]] {

      private var needToRequest = false

      override def onClose(status: Status, trailers: Metadata): Unit = {
        interceptor.onResponseClose(status, trailers)
        closeServerCall(status, trailers)
        GrpcProxyServerCallHandler.this.onCallComplete(
          serverCall.getMethodDescriptor.getFullMethodName,
          channel,
          status)
      }

      override def onHeaders(headers: Metadata): Unit = serverCall.sendHeaders(headers)

      override def onMessage(message: Array[Byte]): Unit = {
        val transformed =
          try {
            interceptor.transformResponse(message)
          } catch {
            case e: Throwable =>
              clientCall.cancel("Failed to transform the response message", e)
              closeServerCall(
                Status.INTERNAL.withDescription(
                  s"Failed to transform the response message: ${e.getMessage}"),
                new Metadata())
              return
          }
        serverCall.sendMessage(transformed)
        this.synchronized {
          if (serverCall.isReady) {
            clientCall.request(1)
          } else {
            needToRequest = true
          }
        }
      }

      override def onReady(): Unit = serverCallListener.onClientReady()

      def onServerReady(): Unit = this.synchronized {
        if (needToRequest) {
          clientCall.request(1)
          needToRequest = false
        }
      }
    }
  }
}
