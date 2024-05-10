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
package org.apache.kyuubi.grpc.session

import io.grpc.stub.StreamObserver
import org.apache.kyuubi.grpc.operation.{GrpcOperation, OperationKey}
import org.apache.kyuubi.grpc.spark.proto.{AddArtifactsRequest, AddArtifactsResponse, AnalyzePlanRequest, AnalyzePlanResponse, ArtifactStatusesRequest, ArtifactStatusesResponse, ConfigRequest, ConfigResponse, ExecutePlanRequest, ExecutePlanResponse, FetchErrorDetailsRequest, FetchErrorDetailsResponse, InterruptRequest, InterruptResponse, ReattachExecuteRequest, ReleaseExecuteRequest, ReleaseExecuteResponse, ReleaseSessionRequest, ReleaseSessionResponse}

import java.nio.file.Path

case class SessionKey(userId: String, sessionId: String)
trait GrpcSession {
  def sessionKey: SessionKey
  def name: Option[String]

  def serverSessionId: String

  def createTime: Long
  def lastAccessTime: Long
  def lastIdleTime: Long

  def sessionManager: GrpcSessionManager

  def open()
  def close()

  def addExecuteOperation(operation: GrpcOperation): OperationKey
  def removeExecuteOperation(operation: GrpcOperation)
  def removeAllOperations(): Unit

  def getOperation(operationKey: OperationKey): GrpcOperation
  def interruptAll(): Seq[String]
  def interruptTag(): Seq[String]
  def interruptOperation(operationId: String): Seq[String]

  def executePlan(request: ExecutePlanRequest,
                  responseObserver: StreamObserver[ExecutePlanResponse]): OperationKey

  def analyzePlan(request: AnalyzePlanRequest,
                  responseObserver: StreamObserver[AnalyzePlanResponse])

  def config(request: ConfigRequest,
             responseObserver: StreamObserver[ConfigResponse])

  def addArtifacts(responseObserver: StreamObserver[AddArtifactsResponse]): StreamObserver[AddArtifactsRequest]

  def artifactStatus(request: ArtifactStatusesRequest,
                     responseObserver: StreamObserver[ArtifactStatusesResponse])

  def interrupt(request: InterruptRequest,
                responseObserver: StreamObserver[InterruptResponse])

  def reattachExecute(request: ReattachExecuteRequest,
                      responseObserver: StreamObserver[ExecutePlanResponse]): OperationKey

  def releaseExecute(request: ReleaseExecuteRequest,
                     responseObserver: StreamObserver[ReleaseExecuteResponse])

  def releaseSession(request: ReleaseSessionRequest,
                     responseObserver: StreamObserver[ReleaseSessionResponse])

  def fetchErrorDetails(request: FetchErrorDetailsRequest,
                        responseObserver: StreamObserver[FetchErrorDetailsResponse])


}
