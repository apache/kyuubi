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

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.log.LogDivertAppender
import org.apache.kyuubi.service.AbstractService

/**
 * The [[GrpcOperationManager]] manages all the grpc operations during their lifecycle
 */
abstract class GrpcOperationManager(name: String)
  extends AbstractService(name) {

  private val keyToOperations = new ConcurrentHashMap[OperationKey, GrpcOperation]

  protected def skipOperationLog: Boolean = false
  def getOperationCount: Int = keyToOperations.size()

  def allOperations(): Iterable[GrpcOperation] = keyToOperations.values().asScala

  override def initialize(conf: KyuubiConf): Unit = {
    LogDivertAppender.initialize(skipOperationLog)
    super.initialize(conf)
  }

  def close(operationKey: OperationKey): Unit = {
    val operation = keyToOperations.get(operationKey)
    if (operation == null) throw KyuubiSQLException(s"Invalid $operationKey")
    operation.close()
  }

  final def addOperation(grpcOperation: GrpcOperation): GrpcOperation = synchronized {
    keyToOperations.put(grpcOperation.operationKey, grpcOperation)
    grpcOperation
  }

  @throws[KyuubiSQLException]
  final def getOperation(operationKey: OperationKey): GrpcOperation = {
    val operation = synchronized { keyToOperations.get(operationKey) }
    if (operation == null) throw KyuubiSQLException(s"Invalid $operationKey")
    operation
  }

  @throws[KyuubiSQLException]
  final def removeOperation(operationKey: OperationKey): GrpcOperation = synchronized {
    val operation = keyToOperations.remove(operationKey)
    if (operation == null) throw KyuubiSQLException(s"Invalid $operationKey")
    operation
  }

  @throws[KyuubiSQLException]
  final def closeOperation(operationKey: OperationKey): Unit = {
    val operation = removeOperation(operationKey)
    operation.close()
  }

  @throws[KyuubiSQLException]
  final def interruptOperation(operationKey: OperationKey): Unit = {
    val operation = getOperation(operationKey)
    operation.interrupt()
  }

}
