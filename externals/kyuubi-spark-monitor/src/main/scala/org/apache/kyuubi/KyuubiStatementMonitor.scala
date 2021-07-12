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

package org.apache.kyuubi

import java.util.concurrent.ConcurrentHashMap

import org.apache.kyuubi.entity.KyuubiStatementInfo

object KyuubiStatementMonitor {

  private val operationStatementMap = new ConcurrentHashMap[String, KyuubiStatementInfo]()

  def addStatementDetailForOperationId(
      operationId: String, kyuubiStatementInfo: KyuubiStatementInfo): Unit = {
    operationStatementMap.putIfAbsent(operationId, kyuubiStatementInfo)
  }

  def addEachStateTimeForOperationid(operationId: String, state: String, time: Long): Unit = {
    operationStatementMap.get(operationId).stateTimeMap.put(state, time)
  }

  def addOperationExceptionByOperationId(operationId: String, operationException: String): Unit = {
    operationStatementMap.get(operationId).exception = operationException
  }

  def addPhysicalplanByOperationId(operationId: String, queryExecution: String): Unit = {
    operationStatementMap.get(operationId).physicalPlan = queryExecution
  }
}
