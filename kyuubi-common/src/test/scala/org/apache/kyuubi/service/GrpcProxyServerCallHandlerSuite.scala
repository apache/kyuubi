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

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Collections
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import io.grpc._
import io.grpc.MethodDescriptor.MethodType
import io.grpc.netty.NettyServerBuilder

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.service.GrpcProxyServerCallHandler.byteMarshaller

class GrpcProxyServerCallHandlerSuite extends KyuubiFunSuite {

  private var backendServer: Server = _
  private var proxyServer: Server = _
  private var backendChannel: ManagedChannel = _
  private var proxyChannel: ManagedChannel = _

  override def afterAll(): Unit = {
    Option(proxyChannel).foreach { channel =>
      channel.shutdownNow()
      channel.awaitTermination(10, TimeUnit.SECONDS)
    }
    Option(backendChannel).foreach { channel =>
      channel.shutdownNow()
      channel.awaitTermination(10, TimeUnit.SECONDS)
    }
    Option(proxyServer).foreach { server =>
      server.shutdownNow()
      server.awaitTermination(10, TimeUnit.SECONDS)
    }
    Option(backendServer).foreach { server =>
      server.shutdownNow()
      server.awaitTermination(10, TimeUnit.SECONDS)
    }
    super.afterAll()
  }

  private def startServers(handler: ServerCallHandler[Array[Byte], Array[Byte]]): Unit = {
    backendServer = NettyServerBuilder.forPort(0)
      .addService(echoServiceDefinition())
      .fallbackHandlerRegistry(
        new GrpcProxyServerCallHandler.PassthroughHandlerRegistry(echoHandler))
      .build()
      .start()
    backendChannel = ManagedChannelBuilder
      .forAddress("localhost", backendServer.getPort)
      .usePlaintext()
      .build()
    proxyServer = NettyServerBuilder.forPort(0)
      .fallbackHandlerRegistry(
        new GrpcProxyServerCallHandler.PassthroughHandlerRegistry(handler))
      .build()
      .start()
    proxyChannel = ManagedChannelBuilder
      .forAddress("localhost", proxyServer.getPort)
      .usePlaintext()
      .build()
  }

  private def invoke(
      methodName: String,
      methodType: MethodType,
      payloads: Seq[String]): (Status, Seq[String]) = {
    val methodDescriptor = MethodDescriptor.newBuilder(byteMarshaller, byteMarshaller)
      .setFullMethodName(methodName)
      .setType(methodType)
      .build()
    val call = proxyChannel.newCall(methodDescriptor, CallOptions.DEFAULT)
    val latch = new CountDownLatch(1)
    val received = Collections.synchronizedList(new java.util.ArrayList[String]())
    var status: Status = null
    call.start(
      new ClientCall.Listener[Array[Byte]] {
        override def onMessage(message: Array[Byte]): Unit = {
          received.add(new String(message, UTF_8))
        }
        override def onClose(st: Status, trailers: Metadata): Unit = {
          status = st
          latch.countDown()
        }
      },
      new Metadata())
    call.request(100)
    payloads.foreach(payload => call.sendMessage(payload.getBytes(UTF_8)))
    call.halfClose()
    assert(latch.await(10, TimeUnit.SECONDS))
    (status, received.asScala.toSeq)
  }

  test("proxy unary, server-streaming, client-streaming, bidi and unknown calls") {
    val resolverCalls = new AtomicInteger(0)
    startServers(new GrpcProxyServerCallHandler {
      override def resolve(
          serverCall: ServerCall[Array[Byte], Array[Byte]],
          firstMessage: Array[Byte]): ResolvedProxyCall = {
        resolverCalls.incrementAndGet()
        ResolvedProxyCall(backendChannel)
      }
    })

    val (status1, responses1) = invoke("test.Service/Unary", MethodType.UNARY, Seq("a"))
    assert(status1.isOk && responses1 == Seq("a") && resolverCalls.get == 1)

    val (status2, responses2) =
      invoke("test.Service/ServerStream", MethodType.SERVER_STREAMING, Seq("b"))
    assert(status2.isOk && responses2 == Seq("b#0", "b#1", "b#2") && resolverCalls.get == 2)

    val (status3, responses3) = invoke(
      "test.Service/ClientStream",
      MethodType.CLIENT_STREAMING,
      Seq("c1", "c2", "c3"))
    assert(status3.isOk && responses3 == Seq("c1,c2,c3") && resolverCalls.get == 3)

    val (status4, responses4) =
      invoke("test.Service/Bidi", MethodType.BIDI_STREAMING, Seq("d1", "d2", "d3"))
    assert(status4.isOk && responses4 == Seq("d1", "d2", "d3") && resolverCalls.get == 4, status4)

    // An RPC of an unknown service is routed through the fallback registry as-is.
    val (status5, responses5) =
      invoke("ext.Plugin/Custom", MethodType.SERVER_STREAMING, Seq("e"))
    assert(status5.isOk && responses5 == Seq("e") && resolverCalls.get == 5)
  }

  test("proxy resolves lazily with the first message and propagates status") {
    val firstMessages = Collections.synchronizedList(new java.util.ArrayList[String]())
    startServers(new GrpcProxyServerCallHandler {
      override def resolve(
          serverCall: ServerCall[Array[Byte], Array[Byte]],
          firstMessage: Array[Byte]): ResolvedProxyCall = {
        firstMessages.add(new String(firstMessage, UTF_8))
        ResolvedProxyCall(backendChannel)
      }
    })

    val (status, responses) =
      invoke("test.Service/ClientStream", MethodType.CLIENT_STREAMING, Seq("c1", "c2", "c3"))
    assert(status.isOk && responses == Seq("c1,c2,c3"))
    assert(firstMessages.asScala.toSeq == Seq("c1"))

    val (failStatus, _) = invoke("test.Service/Fail", MethodType.UNARY, Seq("x"))
    assert(failStatus.getCode == Status.Code.PERMISSION_DENIED)
  }

  test("proxy applies request and response interceptors") {
    startServers(new GrpcProxyServerCallHandler {
      override def resolve(
          serverCall: ServerCall[Array[Byte], Array[Byte]],
          firstMessage: Array[Byte]): ResolvedProxyCall = {
        ResolvedProxyCall(
          backendChannel,
          new GrpcCallProxyInterceptor {
            override def transformRequest(message: Array[Byte]): Array[Byte] = {
              (new String(message, UTF_8) + "?").getBytes(UTF_8)
            }
            override def transformResponse(message: Array[Byte]): Array[Byte] = {
              ("!" + new String(message, UTF_8)).getBytes(UTF_8)
            }
          })
      }
    })

    val (status, responses) = invoke("test.Service/Unary", MethodType.UNARY, Seq("a"))
    assert(status.isOk && responses == Seq("!a?"))
  }

  private def echoServiceDefinition(): ServerServiceDefinition = {
    val builder = ServerServiceDefinition.builder("test.Service")
    addEchoMethod(builder, "Unary", MethodType.UNARY)
    addEchoMethod(builder, "ServerStream", MethodType.SERVER_STREAMING)
    addEchoMethod(builder, "ClientStream", MethodType.CLIENT_STREAMING)
    addEchoMethod(builder, "Bidi", MethodType.BIDI_STREAMING)
    builder.build()
  }

  private def addEchoMethod(
      builder: ServerServiceDefinition.Builder,
      methodName: String,
      methodType: MethodType): Unit = {
    val methodDescriptor = MethodDescriptor.newBuilder(byteMarshaller, byteMarshaller)
      .setFullMethodName(s"test.Service/$methodName")
      .setType(methodType)
      .build()
    builder.addMethod(methodDescriptor, echoHandler)
  }

  private val echoHandler = new ServerCallHandler[Array[Byte], Array[Byte]] {
    override def startCall(
        call: ServerCall[Array[Byte], Array[Byte]],
        headers: Metadata): ServerCall.Listener[Array[Byte]] = {
      new ServerCall.Listener[Array[Byte]] {
        private val received = new java.util.ArrayList[Array[Byte]]()
        private var headersSent = false
        call.request(1)

        override def onMessage(message: Array[Byte]): Unit = {
          received.add(message)
          def sendHeadersOnce(): Unit = if (!headersSent) {
            call.sendHeaders(new Metadata())
            headersSent = true
          }
          call.getMethodDescriptor.getType match {
            case MethodType.CLIENT_STREAMING =>
              call.request(1)
            case MethodType.BIDI_STREAMING =>
              sendHeadersOnce()
              call.sendMessage(message)
              call.request(1)
            case _ => // the response is sent on half-close
          }
        }

        override def onHalfClose(): Unit = {
          val methodName = call.getMethodDescriptor.getBareMethodName
          if (methodName == "Fail") {
            call.close(Status.PERMISSION_DENIED, new Metadata())
          } else {
            if (!headersSent) {
              call.sendHeaders(new Metadata())
              headersSent = true
            }
            call.getMethodDescriptor.getType match {
              case MethodType.UNARY =>
                call.sendMessage(received.get(0))
              case MethodType.SERVER_STREAMING =>
                (0 until 3).foreach { i =>
                  call.sendMessage((new String(received.get(0), UTF_8) + s"#$i").getBytes(UTF_8))
                }
              case MethodType.CLIENT_STREAMING =>
                val combined = received.asScala.map(new String(_, UTF_8)).mkString(",")
                call.sendMessage(combined.getBytes(UTF_8))
              case MethodType.BIDI_STREAMING => // messages have been echoed already
              case _ => // unknown service method, echo the received messages
                received.asScala.foreach(call.sendMessage)
            }
            call.close(Status.OK, new Metadata())
          }
        }
      }
    }
  }
}
