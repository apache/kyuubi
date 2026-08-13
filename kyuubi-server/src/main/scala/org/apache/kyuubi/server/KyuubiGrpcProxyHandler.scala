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

import java.net.SocketAddress
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import com.google.protobuf.{Descriptors, DynamicMessage}
import io.grpc.{Channel, Grpc, ServerCall, Status}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.service.{GrpcCallProxyInterceptor, GrpcProxyServerCallHandler, ResolvedProxyCall}
import org.apache.kyuubi.session.{GrpcSessionHandle, KyuubiGrpcSession, KyuubiGrpcSessionManager}
import org.apache.kyuubi.shaded.spark.connect.proto

/**
 * A [[GrpcProxyServerCallHandler]] that routes every Spark Connect RPC, including
 * methods of unknown services, to the engine of the corresponding Kyuubi session.
 *
 * The session is resolved from the `user_id` and `session_id` fields of the first
 * request message. RPCs that carry no session key, e.g. methods introduced by
 * extension plugins, are routed to the session previously resolved on the same
 * connection.
 *
 * Known Spark Connect messages are decoded and rewritten through a
 * [[GrpcCallProxyInterceptor]], which normalizes the user identity and validates
 * the server-side session id, while unknown messages are forwarded as raw bytes.
 */
class KyuubiGrpcProxyHandler(sessionManager: KyuubiGrpcSessionManager)
  extends GrpcProxyServerCallHandler with Logging {

  private val connectionSessions = new ConcurrentHashMap[SocketAddress, KyuubiGrpcSession]()
  private val channelSessions = new ConcurrentHashMap[Channel, KyuubiGrpcSession]()

  override def resolve(
      serverCall: ServerCall[Array[Byte], Array[Byte]],
      firstMessage: Array[Byte]): ResolvedProxyCall = {
    val methodName = serverCall.getMethodDescriptor.getFullMethodName
    val session = sessionKey(methodName, firstMessage) match {
      case Some((userId, sessionId)) =>
        val s = sessionManager.getOrCreateSession(new GrpcSessionHandle(userId, sessionId), None)
        val remote = remoteAddress(serverCall)
        if (remote != null) {
          connectionSessions.put(remote, s)
        }
        channelSessions.put(s.client.channel, s)
        s
      case None =>
        val remote = remoteAddress(serverCall)
        val s = if (remote != null) connectionSessions.get(remote) else null
        if (s == null) {
          throw Status.UNIMPLEMENTED
            .withDescription(s"Unable to resolve the engine for $methodName without a session key")
            .asRuntimeException()
        }
        s
    }
    val interceptor = sparkMethod(methodName)
      .map { m => new SparkConnectCallInterceptor(m._1, session) }
      .getOrElse(GrpcCallProxyInterceptor.NOOP)
    ResolvedProxyCall(session.client.channel, interceptor)
  }

  override def onCallComplete(methodName: String, channel: Channel, status: Status): Unit = {
    val session = channelSessions.get(channel)
    if (session != null &&
      methodName == proto.SparkConnectServiceGrpc.getReleaseSessionMethod.getFullMethodName &&
      status.isOk) {
      channelSessions.remove(channel)
      val iterator = connectionSessions.entrySet().iterator()
      while (iterator.hasNext) {
        if (iterator.next().getValue eq session) {
          iterator.remove()
        }
      }
      try {
        sessionManager.closeSession(session.handle)
      } catch {
        case e: Throwable => warn(s"Failed to close session ${session.handle}", e)
      }
    }
  }

  private def remoteAddress(
      serverCall: ServerCall[Array[Byte], Array[Byte]]): SocketAddress = {
    serverCall.getAttributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR)
  }

  /**
   * Decodes the first request message of a known Spark Connect method to read the
   * session key. Unknown methods are left to the connection-scoped session.
   */
  private def sessionKey(
      methodName: String,
      firstMessage: Array[Byte]): Option[(String, String)] = {
    sparkMethod(methodName).flatMap { case (inputType, _) =>
      try {
        val message = DynamicMessage.parseFrom(inputType, firstMessage)
        val sessionField = inputType.findFieldByName("session_id")
        val userContextField = inputType.findFieldByName("user_context")
        if (!message.hasField(sessionField) || !message.hasField(userContextField)) {
          None
        } else {
          val sessionId = message.getField(sessionField).asInstanceOf[String]
          val userContext = message.getField(userContextField).asInstanceOf[DynamicMessage]
          val userField = userContext.getDescriptorForType.findFieldByName("user_id")
          val userId =
            if (userField == null || !userContext.hasField(userField)) {
              ""
            } else {
              userContext.getField(userField).asInstanceOf[String]
            }
          if (sessionId.nonEmpty && userId.nonEmpty) Some((userId, sessionId)) else None
        }
      } catch {
        case e: Throwable =>
          warn(s"Failed to decode the session key from $methodName", e)
          None
      }
    }
  }

  private def sparkMethod(
      methodName: String): Option[(Descriptors.Descriptor, Descriptors.Descriptor)] = {
    proto.SparkConnectServiceGrpc.getServiceDescriptor.getMethods.asScala
      .find(_.getFullMethodName == methodName)
      .map { method =>
        val serviceDescriptor = proto.Base.getDescriptor.findServiceByName("SparkConnectService")
        val methodDescriptor = serviceDescriptor.findMethodByName(method.getBareMethodName)
        (methodDescriptor.getInputType, methodDescriptor.getOutputType)
      }
  }

  /**
   * Rewrites the user identity to the resolved session user and validates the
   * server-side session id on the responses.
   */
  private class SparkConnectCallInterceptor(
      inputType: Descriptors.Descriptor,
      session: KyuubiGrpcSession) extends GrpcCallProxyInterceptor {

    private val sessionField = inputType.findFieldByName("session_id")
    private val userContextField = inputType.findFieldByName("user_context")

    override def transformRequest(message: Array[Byte]): Array[Byte] = {
      val request = DynamicMessage.parseFrom(inputType, message)
      if (sessionField == null || userContextField == null || !request.hasField(userContextField)) {
        message
      } else {
        val builder = request.toBuilder
        var changed = false
        if (!request.hasField(sessionField)) {
          builder.setField(sessionField, session.handle.sessionId)
          changed = true
        }
        val userContext = request.getField(userContextField).asInstanceOf[DynamicMessage]
        val userDescriptor = userContext.getDescriptorForType
        val userIdField = userDescriptor.findFieldByName("user_id")
        val userNameField = userDescriptor.findFieldByName("user_name")
        if (userIdField != null && userNameField != null &&
          (userContext.getField(userIdField).asInstanceOf[String] != session.user ||
            userContext.getField(userNameField).asInstanceOf[String] != session.user)) {
          val userContextBuilder = userContext.toBuilder
          userContextBuilder.setField(userIdField, session.user)
          userContextBuilder.setField(userNameField, session.user)
          builder.setField(userContextField, userContextBuilder.build())
          changed = true
        }
        if (!changed) {
          message
        } else {
          builder.build().toByteArray
        }
      }
    }
  }
}
