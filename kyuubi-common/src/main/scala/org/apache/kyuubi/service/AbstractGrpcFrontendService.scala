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

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import com.google.protobuf.Message
import io.grpc._
import io.grpc.MethodDescriptor.PrototypeMarshaller
import io.grpc.netty.NettyServerBuilder
import io.grpc.protobuf.ProtoUtils
import io.grpc.protobuf.services.ProtoReflectionService

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.shaded.spark.connect.proto
import org.apache.kyuubi.util.{JavaUtils, NamedThreadFactory}

abstract class AbstractGrpcFrontendService(name: String)
  extends AbstractFrontendService(name) with Runnable with BindableService with Logging {

  private val started = new AtomicBoolean(false)
  protected var server: Server = _
  protected def portNum: Int = conf.get(FRONTEND_GRPC_BIND_PORT)
  protected def maxInboundMessageSize: Int = conf.get(FRONTEND_GRPC_MAX_MESSAGE_SIZE)

  protected def serverHost: Option[String] = conf.get(FRONTEND_GRPC_BIND_HOST)
  protected lazy val serverAddr: InetAddress =
    serverHost.map(InetAddress.getByName).getOrElse(JavaUtils.findLocalInetAddress)

  private lazy val serverThread = new NamedThreadFactory(getName, false).newThread(this)

  def sparkConnectAsyncService: proto.SparkConnectServiceGrpc.AsyncService

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf
    try {
      val socketAddress = new InetSocketAddress(serverAddr.getHostName, portNum)
      val nettyServerBuilder = NettyServerBuilder
        .forAddress(socketAddress)
        .maxInboundMessageSize(maxInboundMessageSize)
        .addService(ProtoReflectionService.newInstance)
        .addService(this)
      server = nettyServerBuilder.build()
      info(s"gRPC frontend service has started at sc://${serverAddr.getHostName}:$portNum")
    } catch {
      case e: Throwable =>
        error(e)
        throw new KyuubiException(
          s"Failed to initialize gRPC frontend service on ${serverAddr.getHostName}:$portNum",
          e)
    }
    super.initialize(conf)
  }

  override def start(): Unit = {
    try {
      if (started.compareAndSet(false, true)) {
        serverThread.start()
      }
      super.start()
    } catch {
      case e: Throwable =>
        stopInternal()
        throw e
    }
  }

  private def stopInternal(): Unit = {
    if (started.compareAndSet(true, false)) {
      serverThread.interrupt()
      stopServer(Some(10L), Some(TimeUnit.SECONDS))
      info(s"$getName has stoppped")
    }
  }

  override def stop(): Unit = {
    super.stop()
    stopInternal()
  }

  def stopServer(timeout: Option[Long] = None, unit: Option[TimeUnit] = None): Unit = {
    if (server != null) {
      if (timeout.isDefined && unit.isDefined) {
        server.shutdown()
        server.awaitTermination(timeout.get, unit.get)
      } else {
        server.shutdown()
      }
    }
  }

  override def run(): Unit = {
    try {
      server.start()
      info("gRPC Server Start Success")
    } catch {
      case _: InterruptedException => error(s"$getName is interrupted")
      case t: Throwable =>
        error(s"Error starting $getName", t)
        System.exit(-1)
    }
  }

  override def connectionUrl: String = {
    val host = (conf.get(FRONTEND_ADVERTISED_HOST), serverHost) match {
      case (Some(advertisedHost), _) => advertisedHost
      case (None, Some(h)) => h
      case (None, None) => serverAddr.getHostAddress
    }
    s"sc://$host:$portNum"
  }

  protected def isServer: Boolean

  override def bindService(): ServerServiceDefinition = {
    // First, get the SparkConnectService ServerServiceDefinition.
    val serviceDef = proto.SparkConnectServiceGrpc.bindService(sparkConnectAsyncService)

    // Create a new ServerServiceDefinition builder
    // using the name of the original service definition.
    val builder = ServerServiceDefinition.builder(serviceDef.getServiceDescriptor.getName)

    // Iterate through all the methods of the original service definition.
    // For each method, add a customized method descriptor (with updated marshallers)
    // and the original server call handler to the builder.
    serviceDef.getMethods.asScala
      .asInstanceOf[Iterable[ServerMethodDefinition[Message, Message]]]
      .foreach { method =>
        builder.addMethod(
          methodWithCustomMarshallers(method.getMethodDescriptor),
          method.getServerCallHandler)
      }

    // Build the final ServerServiceDefinition and return it.
    builder.build()
  }

  private def methodWithCustomMarshallers(methodDesc: MethodDescriptor[Message, Message])
      : MethodDescriptor[Message, Message] = {
    val recursionLimit = 1024
    val requestMarshaller = {
      ProtoUtils.marshallerWithRecursionLimit(
        methodDesc.getRequestMarshaller
          .asInstanceOf[PrototypeMarshaller[Message]]
          .getMessagePrototype,
        recursionLimit)
    }
    val responseMarshaller =
      ProtoUtils.marshallerWithRecursionLimit(
        methodDesc.getResponseMarshaller
          .asInstanceOf[PrototypeMarshaller[Message]]
          .getMessagePrototype,
        recursionLimit)
    methodDesc.toBuilder
      .setRequestMarshaller(requestMarshaller)
      .setResponseMarshaller(responseMarshaller)
      .build()
  }
}
