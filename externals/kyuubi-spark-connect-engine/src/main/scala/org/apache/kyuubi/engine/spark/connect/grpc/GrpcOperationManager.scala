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

import java.util

import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter

import io.grpc.stub.StreamObserver

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.connect.grpc.proto.{ConfigRequest, ConfigResponse}
import org.apache.kyuubi.operation.{Operation, OperationHandle}
import org.apache.kyuubi.operation.OperationState.{CANCELED, CLOSED, ERROR, FINISHED, UNKNOWN}
import org.apache.kyuubi.operation.log.LogDivertAppender
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.session.Session

abstract class GrpcOperationManager(name: String) extends AbstractService(name) {
  final private val handleToOperation = new util.HashMap[OperationHandle, Operation]()
  def getOperationCount: Int = handleToOperation.size()
  def allOperations(): Iterable[Operation] = handleToOperation.values().asScala
  protected def skipOperationLog: Boolean = false

  override def initialize(conf: KyuubiConf): Unit = {
    LogDivertAppender.initialize(skipOperationLog)
    super.initialize(conf)
  }

  def newConfigOperation(
      session: Session,
      request: ConfigRequest,
      response: StreamObserver[ConfigResponse]): Operation

}
