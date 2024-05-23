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

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.google.protobuf.MessageLite
import io.grpc._
import io.grpc.MethodDescriptor.PrototypeMarshaller
import io.grpc.netty.NettyServerBuilder
import io.grpc.protobuf.lite.ProtoLiteUtils

import org.apache.kyuubi.{KyuubiException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_SPARK_CONNECT_GRPC_BINDING_PORT, FRONTEND_ADVERTISED_HOST}
import org.apache.kyuubi.service.CompositeService
import org.apache.kyuubi.util.NamedThreadFactory

abstract class AbstractGrpcFrontendService(name: String)
  extends CompositeService(name) with GrpcFrontendService with Runnable
  with BindableService with Logging {

  private val started = new AtomicBoolean(false)
  protected var server: Server = _
  protected def portNum: Int = conf.get(ENGINE_SPARK_CONNECT_GRPC_BINDING_PORT)
  protected def maxInboundMessageSize: Int = 1024

  protected def serverHost: Option[String]
  protected lazy val serverAddr: InetAddress =
    serverHost.map(InetAddress.getByName).getOrElse(Utils.findLocalInetAddress)

  private lazy val serverThread = new NamedThreadFactory(getName, false).newThread(this)

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf
    try {
      val socketAddress = new InetSocketAddress(serverAddr.getHostName, portNum)
      val nettyServerBuilder = NettyServerBuilder
        .forAddress(socketAddress)
        .maxInboundMessageSize(maxInboundMessageSize)
        .addService(this)
      server = nettyServerBuilder.build()
    } catch {
      case e: Throwable =>
        error(e)
        throw new KyuubiException(
          s"Failed to initialize grpc frontend service on $portNum",
          e)
    }
    super.initialize(conf)
  }

  override def bindService(): ServerServiceDefinition

  protected def methodWithCustomMarshallers(methodDesc: MethodDescriptor[MessageLite, MessageLite])
      : MethodDescriptor[MessageLite, MessageLite] = {
    // default 1024
    val recursionLimit = 1024
    val requestMarshaller =
      ProtoLiteUtils.marshallerWithRecursionLimit(
        methodDesc.getRequestMarshaller
          .asInstanceOf[PrototypeMarshaller[MessageLite]]
          .getMessagePrototype,
        recursionLimit)
    val responseMarshaller =
      ProtoLiteUtils.marshallerWithRecursionLimit(
        methodDesc.getResponseMarshaller
          .asInstanceOf[PrototypeMarshaller[MessageLite]]
          .getMessagePrototype,
        recursionLimit)
    methodDesc.toBuilder
      .setRequestMarshaller(requestMarshaller)
      .setResponseMarshaller(responseMarshaller)
      .build()
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
      info(getName + " has stoppped")
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
      info("Grpc Server Start Success")
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
    host + ":" + portNum
  }

  protected def isServer(): Boolean = false

}
