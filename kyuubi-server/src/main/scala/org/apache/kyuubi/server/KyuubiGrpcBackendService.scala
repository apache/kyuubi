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

import io.grpc.stub.StreamObserver

import org.apache.kyuubi.Logging
import org.apache.kyuubi.service.AbstractBackendService
import org.apache.kyuubi.session.{GrpcSessionHandle, KyuubiGrpcSession, KyuubiGrpcSessionManager}
import org.apache.kyuubi.shaded.spark.connect.proto

class KyuubiGrpcBackendService extends AbstractBackendService("KyuubiGrpcBackendService")
  with proto.SparkConnectServiceGrpc.AsyncService with Logging {

  override val sessionManager: KyuubiGrpcSessionManager = new KyuubiGrpcSessionManager()

  override def executePlan(
      req: proto.ExecutePlanRequest,
      respObserver: StreamObserver[proto.ExecutePlanResponse]): Unit = {
    info(s"executePlan - session_id: ${req.getSessionId}, operation_id: ${req.getOperationId}")
    val previousSessionId = if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else None
    val session = sessionManager.getOrCreateSession(
      new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
      previousSessionId)
    session.client.astub.executePlan(req, respObserver)
  }

  override def analyzePlan(
      req: proto.AnalyzePlanRequest,
      respObserver: StreamObserver[proto.AnalyzePlanResponse]): Unit = {
    warn(s"analyzePlan - session_id: ${req.getSessionId}")
    val previousSessionId = if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else None
    val session = sessionManager.getOrCreateSession(
      new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
      previousSessionId)
    session.client.astub.analyzePlan(req, respObserver)
  }

  override def config(
      req: proto.ConfigRequest,
      respObserver: StreamObserver[proto.ConfigResponse]): Unit = {
    warn(s"config - session_id: ${req.getSessionId}")
    val previousSessionId = if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else None
    val session = sessionManager.getOrCreateSession(
      new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
      previousSessionId)
    session.client.astub.config(req, respObserver)
  }

  // FIXME this is dummy implementation to discard any uploaded artifacts
  override def addArtifacts(respObserver: StreamObserver[proto.AddArtifactsResponse])
      : StreamObserver[proto.AddArtifactsRequest] = {
    warn(s"addArtifacts")
    new StreamObserver[proto.AddArtifactsRequest] {

      private var grcpSession: KyuubiGrpcSession = _

      override def onNext(req: proto.AddArtifactsRequest): Unit = {
        if (grcpSession == null) {
          grcpSession = sessionManager.getOrCreateSession(
            new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
            Option(req.getClientObservedServerSideSessionId))
          grcpSession.client.astub.addArtifacts(respObserver)
        }
      }

      override def onError(t: Throwable): Unit = {
        respObserver.onError(t)
      }

      override def onCompleted(): Unit = {
        val builder = proto.AddArtifactsResponse.newBuilder()
        builder.setSessionId(grcpSession.handle.sessionId)
        builder.setServerSideSessionId(grcpSession.handle.sessionId)
        respObserver.onNext(builder.build())
        respObserver.onCompleted()
      }
    }
  }

  override def artifactStatus(
      req: proto.ArtifactStatusesRequest,
      respObserver: StreamObserver[proto.ArtifactStatusesResponse]): Unit = {
    warn(s"artifactStatus - session_id: ${req.getSessionId}")
    val previousSessionId = if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else None
    val session = sessionManager.getOrCreateSession(
      new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
      previousSessionId)
    session.client.astub.artifactStatus(req, respObserver)
  }

  override def interrupt(
      req: proto.InterruptRequest,
      respObserver: StreamObserver[proto.InterruptResponse]): Unit = {
    warn(s"interrupt - session_id: ${req.getSessionId}")
    val previousSessionId = if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else None
    val session = sessionManager.getOrCreateSession(
      new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
      previousSessionId)
    session.client.astub.interrupt(req, respObserver)
  }

  override def reattachExecute(
      req: proto.ReattachExecuteRequest,
      respObserver: StreamObserver[proto.ExecutePlanResponse]): Unit = {
    warn(s"reattachExecute - session_id: ${req.getSessionId}, operation_id: ${req.getOperationId}")
    val previousSessionId = if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else None
    val session = sessionManager.getOrCreateSession(
      new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
      previousSessionId)
    session.client.astub.reattachExecute(req, respObserver)
  }

  override def releaseExecute(
      req: proto.ReleaseExecuteRequest,
      respObserver: StreamObserver[proto.ReleaseExecuteResponse]): Unit = {
    warn(s"releaseExecute - session_id: ${req.getSessionId}, operation_id: ${req.getOperationId}")
    val previousSessionId = if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else None
    val session = sessionManager.getOrCreateSession(
      new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
      previousSessionId)
    session.client.astub.releaseExecute(req, respObserver)
  }

  override def releaseSession(
      req: proto.ReleaseSessionRequest,
      respObserver: StreamObserver[proto.ReleaseSessionResponse]): Unit = {
    warn(s"releaseSession - session_id: ${req.getSessionId}")
    val session = sessionManager.getOrCreateSession(
      new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
      None)
    session.client.astub.releaseSession(req, respObserver)
  }

  override def fetchErrorDetails(
      req: proto.FetchErrorDetailsRequest,
      respObserver: StreamObserver[proto.FetchErrorDetailsResponse]): Unit = {
    warn(s"fetchErrorDetails - session_id: ${req.getSessionId}, error_id: ${req.getErrorId}")
    val previousSessionId = if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else None
    val session = sessionManager.getOrCreateSession(
      new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
      previousSessionId)
    session.client.astub.fetchErrorDetails(req, respObserver)
  }
}
