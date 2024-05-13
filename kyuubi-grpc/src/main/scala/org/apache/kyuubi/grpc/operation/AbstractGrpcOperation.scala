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

import org.apache.kyuubi.{KyuubiSQLException, Logging, Utils}
import org.apache.kyuubi.grpc.session.GrpcSession
import org.apache.kyuubi.config.KyuubiConf._

import java.util.concurrent.locks.ReentrantLock

abstract class AbstractGrpcOperation(session: GrpcSession) extends GrpcOperation  with Logging {
  final protected val opType: String = getClass.getSimpleName
  final protected val createTime = System.currentTimeMillis()
  protected var key: OperationKey = OperationKey(session.sessionKey)
  final private val operationTimeout: Long = {
    session.sessionManager.getConf.get(OPERATION_IDLE_TIMEOUT)
  }

  private var lock: ReentrantLock = new ReentrantLock()

  protected def withLockRequired[T](block: => T): T = Utils.withLockRequired(lock)(block)

  @volatile protected var startTime: Long = _
  @volatile protected var completedTime: Long = _
  @volatile protected var lastAccessTime: Long = createTime

  @volatile protected var operationException: KyuubiSQLException = _

  protected def setOperationException(ex: KyuubiSQLException): Unit = {
    this.operationException = ex
  }




}
