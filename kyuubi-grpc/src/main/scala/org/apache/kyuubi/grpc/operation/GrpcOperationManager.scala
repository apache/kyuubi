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
package org.apache.kyuubi.grpc.operation

import java.util.concurrent._

import scala.collection.JavaConverters._

import io.grpc.stub.StreamObserver

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.grpc.session.GrpcSession
import org.apache.kyuubi.grpc.spark.proto._
import org.apache.kyuubi.operation.log.LogDivertAppender
import org.apache.kyuubi.service.AbstractService

/**
 * The [[GrpcOperationManager]] manages all the grpc operations during their lifecycle
 */
abstract class GrpcOperationManager(name: String) extends AbstractService(name) {

  private val keyToOperations = new ConcurrentHashMap[OperationKey, GrpcOperation]
  private val operationsLock = new Object

  private var lastExecutionTimeMs: Option[Long] = Some(System.currentTimeMillis())

  def getOperationCount: Int = keyToOperations.size()

  def allOperations(): Iterable[GrpcOperation] = keyToOperations.values().asScala

  override def initialize(conf: KyuubiConf): Unit = {
    LogDivertAppender.initialize()
    super.initialize(conf)
  }

  def close(opKey: OperationKey)

  def newExecutePlanOperation(
      session: GrpcSession,
      request: ExecutePlanRequest,
      responseObServer: StreamObserver[ExecutePlanResponse]): GrpcOperation

  def newAnalyzePlanOperation(
      session: GrpcSession,
      request: AnalyzePlanRequest,
      responseObserver: StreamObserver[AnalyzePlanResponse]): GrpcOperation

  def newConfigOperation(
      session: GrpcSession,
      request: ConfigRequest,
      responseObserver: StreamObserver[ConfigResponse]): GrpcOperation

  def newAddArtifactsOperation(
      session: GrpcSession,
      requestStreamObserver: StreamObserver[AddArtifactsResponse])
      : (StreamObserver[AddArtifactsRequest], GrpcOperation)

  def newArtifactStatusOperation(
      session: GrpcSession,
      request: ArtifactStatusesRequest,
      responseObserver: StreamObserver[ArtifactStatusesResponse]): GrpcOperation

  def newInterruptOperation(
      session: GrpcSession,
      request: InterruptRequest,
      responseObserver: StreamObserver[InterruptResponse]): GrpcOperation

  def newReattachExecuteOperation(
      session: GrpcSession,
      request: ReattachExecuteRequest,
      responseObserver: StreamObserver[ExecutePlanResponse]): GrpcOperation

  def newReleaseExecuteOperation(
      session: GrpcSession,
      request: ReleaseExecuteRequest,
      responseObserver: StreamObserver[ReleaseExecuteResponse]): GrpcOperation

  def newReleaseSessionOperation(
      session: GrpcSession,
      request: ReleaseSessionRequest,
      responseObserver: StreamObserver[ReleaseSessionResponse]): GrpcOperation

  def newFetchErrorDetailsOperation(
      session: GrpcSession,
      request: FetchErrorDetailsRequest,
      responseObserver: StreamObserver[FetchErrorDetailsResponse]): GrpcOperation

}
