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
package org.apache.kyuubi.engine.spark.connect.grpc

import io.grpc.stub.StreamObserver

import org.apache.kyuubi.engine.spark.connect.grpc.proto._
import org.apache.kyuubi.service.AbstractBackendService

abstract class AbstractGrpcBackendService(name: String)
  extends AbstractBackendService(name) {

  def executePlan(
      request: ExecutePlanRequest,
      responseObserver: StreamObserver[ExecutePlanResponse])

  def analyzePlan(
      request: AnalyzePlanRequest,
      responseObserver: StreamObserver[AnalyzePlanResponse])

  def config(request: ConfigRequest, responseObserver: StreamObserver[ConfigResponse])

  def addArtifacts(
      request: AddArtifactsRequest,
      responseObserver: StreamObserver[AddArtifactsResponse]): StreamObserver[AddArtifactsResponse]

  def artifactStatus(
      request: ArtifactStatusesRequest,
      responseObserver: StreamObserver[ArtifactStatusesResponse])

  def interrupt(request: InterruptRequest, responseObserver: StreamObserver[InterruptResponse])

  def reattachExecute(
      request: ReattachExecuteRequest,
      responseObserver: StreamObserver[ExecutePlanResponse])

  def releaseExecute(
      request: ReleaseExecuteRequest,
      responseObserver: StreamObserver[ReleaseExecuteResponse])

  def releaseSession(
      request: ReleaseSessionRequest,
      responseObserver: StreamObserver[ReleaseSessionResponse])

  def fetchErrorDetails(
      request: FetchErrorDetailsRequest,
      responseObServer: StreamObserver[FetchErrorDetailsResponse])

}
