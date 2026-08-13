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

import scala.collection.JavaConverters._

import com.google.protobuf.Message
import io.grpc._
import io.grpc.MethodDescriptor.PrototypeMarshaller
import io.grpc.protobuf.ProtoUtils
import io.grpc.stub.StreamObserver

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.service.{AbstractBackendService, GrpcRemoteAddressInterceptor}
import org.apache.kyuubi.session.{GrpcSessionHandle, KyuubiGrpcSession, KyuubiGrpcSessionManager}
import org.apache.kyuubi.shaded.spark.connect.proto

/**
 * The full proxy of the Spark Connect service. Every RPC of the current
 * Spark Connect proto is implemented with typed request and response messages,
 * which allows inspecting and rewriting the traffic before forwarding it to
 * the engine of the resolved session. RPCs of unknown services, e.g. methods
 * introduced by extension plugins, are covered by the byte-level fallback
 * handler in the frontend service.
 */
class KyuubiGrpcBackendService extends AbstractBackendService("KyuubiGrpcBackendService")
  with proto.SparkConnectServiceGrpc.AsyncService with BindableService with Logging {

  override val sessionManager: KyuubiGrpcSessionManager = new KyuubiGrpcSessionManager()

  override def executePlan(
      req: proto.ExecutePlanRequest,
      respObserver: StreamObserver[proto.ExecutePlanResponse]): Unit = forward(respObserver) {
    val session = resolveSession(req.getUserContext, req.getSessionId, observedSessionId(req))
    session.client.astub.executePlan(normalized(req, session), respObserver)
  }

  override def analyzePlan(
      req: proto.AnalyzePlanRequest,
      respObserver: StreamObserver[proto.AnalyzePlanResponse]): Unit = forward(respObserver) {
    val session = resolveSession(req.getUserContext, req.getSessionId, observedSessionId(req))
    session.client.astub.analyzePlan(normalized(req, session), respObserver)
  }

  override def config(
      req: proto.ConfigRequest,
      respObserver: StreamObserver[proto.ConfigResponse]): Unit = forward(respObserver) {
    val session = resolveSession(req.getUserContext, req.getSessionId, observedSessionId(req))
    session.client.astub.config(normalized(req, session), respObserver)
  }

  override def addArtifacts(respObserver: StreamObserver[proto.AddArtifactsResponse])
      : StreamObserver[proto.AddArtifactsRequest] = {
    new StreamObserver[proto.AddArtifactsRequest] {
      private var session: KyuubiGrpcSession = _
      private var engineObserver: StreamObserver[proto.AddArtifactsRequest] = _

      override def onNext(req: proto.AddArtifactsRequest): Unit = {
        try {
          if (session == null) {
            session = resolveSession(req.getUserContext, req.getSessionId, observedSessionId(req))
            engineObserver = session.client.astub.addArtifacts(respObserver)
          }
          engineObserver.onNext(normalized(req, session))
        } catch {
          case t: Throwable => onError(t)
        }
      }

      override def onError(t: Throwable): Unit = {
        if (engineObserver != null) {
          engineObserver.onError(t)
        } else {
          respObserver.onError(t)
        }
      }

      override def onCompleted(): Unit = {
        if (engineObserver != null) {
          engineObserver.onCompleted()
        } else {
          respObserver.onError(
            Status.INTERNAL.withDescription("No artifact chunk received").asRuntimeException())
        }
      }
    }
  }

  override def artifactStatus(
      req: proto.ArtifactStatusesRequest,
      respObserver: StreamObserver[proto.ArtifactStatusesResponse]): Unit = forward(respObserver) {
    val session = resolveSession(req.getUserContext, req.getSessionId, observedSessionId(req))
    session.client.astub.artifactStatus(normalized(req, session), respObserver)
  }

  override def interrupt(
      req: proto.InterruptRequest,
      respObserver: StreamObserver[proto.InterruptResponse]): Unit = forward(respObserver) {
    val session = resolveSession(req.getUserContext, req.getSessionId, observedSessionId(req))
    session.client.astub.interrupt(normalized(req, session), respObserver)
  }

  override def reattachExecute(
      req: proto.ReattachExecuteRequest,
      respObserver: StreamObserver[proto.ExecutePlanResponse]): Unit = forward(respObserver) {
    val session = resolveSession(req.getUserContext, req.getSessionId, observedSessionId(req))
    session.client.astub.reattachExecute(normalized(req, session), respObserver)
  }

  override def releaseExecute(
      req: proto.ReleaseExecuteRequest,
      respObserver: StreamObserver[proto.ReleaseExecuteResponse]): Unit = forward(respObserver) {
    val session = resolveSession(req.getUserContext, req.getSessionId, observedSessionId(req))
    session.client.astub.releaseExecute(normalized(req, session), respObserver)
  }

  override def releaseSession(
      req: proto.ReleaseSessionRequest,
      respObserver: StreamObserver[proto.ReleaseSessionResponse]): Unit = forward(respObserver) {
    val session = resolveSession(req.getUserContext, req.getSessionId, None)
    session.client.astub.releaseSession(
      normalized(req, session),
      new StreamObserver[proto.ReleaseSessionResponse] {
        override def onNext(resp: proto.ReleaseSessionResponse): Unit = respObserver.onNext(resp)

        override def onError(t: Throwable): Unit = respObserver.onError(t)

        override def onCompleted(): Unit = {
          try {
            sessionManager.unregisterConnectionSession(session)
            sessionManager.closeSession(session.handle)
          } finally {
            respObserver.onCompleted()
          }
        }
      })
  }

  override def fetchErrorDetails(
      req: proto.FetchErrorDetailsRequest,
      respObserver: StreamObserver[proto.FetchErrorDetailsResponse]): Unit = forward(respObserver) {
    val session = resolveSession(req.getUserContext, req.getSessionId, observedSessionId(req))
    session.client.astub.fetchErrorDetails(normalized(req, session), respObserver)
  }

  private def resolveSession(
      userContext: proto.UserContext,
      sessionId: String,
      previouslyObservedSessionId: Option[String]): KyuubiGrpcSession = {
    val session =
      if (userContext.getUserId.nonEmpty) {
        val s = sessionManager.getOrCreateSession(
          new GrpcSessionHandle(userContext.getUserId, sessionId),
          previouslyObservedSessionId)
        sessionManager.registerConnectionSession(GrpcRemoteAddressInterceptor.get(), s)
        s
      } else {
        val s = sessionManager.getConnectionSession(GrpcRemoteAddressInterceptor.get())
        if (s == null) {
          throw Status.UNIMPLEMENTED
            .withDescription("Unable to resolve the engine without a user context")
            .asRuntimeException()
        }
        s
      }
    session
  }

  private def normalizeUserContext(
      userContext: proto.UserContext,
      session: KyuubiGrpcSession): proto.UserContext = {
    if (userContext.getUserId == session.user && userContext.getUserName == session.user) {
      userContext
    } else {
      userContext.toBuilder.setUserId(session.user).setUserName(session.user).build()
    }
  }

  private def normalized(
      req: proto.ExecutePlanRequest,
      session: KyuubiGrpcSession): proto.ExecutePlanRequest =
    req.toBuilder.setUserContext(normalizeUserContext(req.getUserContext, session)).build()

  private def normalized(
      req: proto.AnalyzePlanRequest,
      session: KyuubiGrpcSession): proto.AnalyzePlanRequest =
    req.toBuilder.setUserContext(normalizeUserContext(req.getUserContext, session)).build()

  private def normalized(
      req: proto.ConfigRequest,
      session: KyuubiGrpcSession): proto.ConfigRequest =
    req.toBuilder.setUserContext(normalizeUserContext(req.getUserContext, session)).build()

  private def normalized(
      req: proto.AddArtifactsRequest,
      session: KyuubiGrpcSession): proto.AddArtifactsRequest =
    req.toBuilder.setUserContext(normalizeUserContext(req.getUserContext, session)).build()

  private def normalized(
      req: proto.ArtifactStatusesRequest,
      session: KyuubiGrpcSession): proto.ArtifactStatusesRequest =
    req.toBuilder.setUserContext(normalizeUserContext(req.getUserContext, session)).build()

  private def normalized(
      req: proto.InterruptRequest,
      session: KyuubiGrpcSession): proto.InterruptRequest =
    req.toBuilder.setUserContext(normalizeUserContext(req.getUserContext, session)).build()

  private def normalized(
      req: proto.ReattachExecuteRequest,
      session: KyuubiGrpcSession): proto.ReattachExecuteRequest =
    req.toBuilder.setUserContext(normalizeUserContext(req.getUserContext, session)).build()

  private def normalized(
      req: proto.ReleaseExecuteRequest,
      session: KyuubiGrpcSession): proto.ReleaseExecuteRequest =
    req.toBuilder.setUserContext(normalizeUserContext(req.getUserContext, session)).build()

  private def normalized(
      req: proto.ReleaseSessionRequest,
      session: KyuubiGrpcSession): proto.ReleaseSessionRequest =
    req.toBuilder.setUserContext(normalizeUserContext(req.getUserContext, session)).build()

  private def normalized(
      req: proto.FetchErrorDetailsRequest,
      session: KyuubiGrpcSession): proto.FetchErrorDetailsRequest =
    req.toBuilder.setUserContext(normalizeUserContext(req.getUserContext, session)).build()

  private def observedSessionId(req: proto.ExecutePlanRequest): Option[String] =
    if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else {
      None
    }

  private def observedSessionId(req: proto.AnalyzePlanRequest): Option[String] =
    if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else {
      None
    }

  private def observedSessionId(req: proto.ConfigRequest): Option[String] =
    if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else {
      None
    }

  private def observedSessionId(req: proto.AddArtifactsRequest): Option[String] =
    if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else {
      None
    }

  private def observedSessionId(req: proto.ArtifactStatusesRequest): Option[String] =
    if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else {
      None
    }

  private def observedSessionId(req: proto.InterruptRequest): Option[String] =
    if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else {
      None
    }

  private def observedSessionId(req: proto.ReattachExecuteRequest): Option[String] =
    if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else {
      None
    }

  private def observedSessionId(req: proto.ReleaseExecuteRequest): Option[String] =
    if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else {
      None
    }

  private def observedSessionId(req: proto.FetchErrorDetailsRequest): Option[String] =
    if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else {
      None
    }

  private def forward(respObserver: StreamObserver[_])(f: => Unit): Unit = {
    try {
      f
    } catch {
      case e: StatusRuntimeException => respObserver.onError(e)
      case e: Throwable =>
        respObserver.onError(
          Status.INTERNAL.withCause(e).withDescription(KyuubiSQLException(e).getMessage)
            .asRuntimeException())
    }
  }

  override def bindService(): ServerServiceDefinition = {
    val serviceDef = proto.SparkConnectServiceGrpc.bindService(this)

    val builder = ServerServiceDefinition.builder(serviceDef.getServiceDescriptor.getName)

    serviceDef.getMethods.asScala
      .asInstanceOf[Iterable[ServerMethodDefinition[Message, Message]]]
      .foreach { method =>
        builder.addMethod(
          methodWithCustomMarshallers(method.getMethodDescriptor),
          method.getServerCallHandler)
      }

    builder.build()
  }

  private def methodWithCustomMarshallers(methodDesc: MethodDescriptor[Message, Message])
      : MethodDescriptor[Message, Message] = {
    val recursionLimit = 1024
    val requestMarshaller = ProtoUtils.marshallerWithRecursionLimit(
      methodDesc.getRequestMarshaller
        .asInstanceOf[PrototypeMarshaller[Message]]
        .getMessagePrototype,
      recursionLimit)
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
