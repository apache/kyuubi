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

package org.apache.spark.kyuubi

import java.util
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.kyuubi.entity.entity.KStatement

import org.apache.kyuubi.operation.OperationState

object SparkSQLMetrics {

  // TODO: Need to check data in maps is useful
  // When we dump data from operationStatementMap, we need to clear the data
  // in executionOperationMap, executionPhysicalPlanMap and executionStartTimeMap.
  // This action make sure that we will not store unused data in mem.
  private val executionOperationMap = new ConcurrentHashMap[Long, String]()
  private val executionPhysicalPlanMap = new ConcurrentHashMap[Long, String]()
  // This map store each execution startTime
  private val executionStartTimeMap = new ConcurrentHashMap[Long, Long]()

  private val operationStatementMap = new ConcurrentHashMap[String, KStatement]()

  private var endStateList = new util.ArrayList[String]()
  endStateList.add(OperationState.FINISHED.toString)
  endStateList.add(OperationState.CANCELED.toString)
  endStateList.add(OperationState.CLOSED.toString)
  endStateList.add(OperationState.ERROR.toString)
  endStateList.add(OperationState.TIMEOUT.toString)

  def addPhysicalPlanForExecutionId(executionId: Long, physicalPlan: String): Unit = {
    executionPhysicalPlanMap.putIfAbsent(executionId, physicalPlan)
  }

  // It only needs to be executed once for the same statement
  def addExecutionInfoIntoKs(executionId: Long, operatioId: String): Unit = {
    if (executionOperationMap.putIfAbsent(executionId, operatioId) == null) {
      if (executionPhysicalPlanMap.containsKey(executionId)) {
        operationStatementMap.get(operatioId).setPhysicPlan(
          executionPhysicalPlanMap.remove(executionId))
      }
      operationStatementMap.get(operatioId).setExecutionId(executionId)
      if (executionStartTimeMap.containsKey(executionId)) {
        // Add executionStartTimeMap into ks
        addEachStateTimeForOperationid(
          operatioId, OperationState.RUNNING.toString, executionStartTimeMap.remove(executionId))
      }
    }
  }

  def addStatementDetailForOperationId(operationId: String, kStatement: KStatement): Unit = {
    operationStatementMap.putIfAbsent(operationId, kStatement)
  }

  def addStartTimeForExecutionId(executionId: Long, time: Long): Unit = {
    executionStartTimeMap.put(executionId, time)
  }

  def addFinishTimeForExecutionId(executionId: Long, finishTime: Long): Unit = {
    // Get operationId
    val operationId = executionOperationMap.get(executionId)
    if (operationId != null) {
      addEachStateTimeForOperationid(operationId, OperationState.FINISHED.toString, finishTime)
    }
  }

  def addEachStateTimeForOperationid(operationId: String, state: String, time: Long): Unit = {
    operationStatementMap.get(operationId).setStateTime(state, time)
     if (endStateList.contains(state)) {
       // If this statement's state is endState, we should remove data from executionOperationMap
       executionOperationMap.remove(operationStatementMap.get(operationId).getExecutionId)
     }
  }

  def operationIsExist(executionId: Long): Boolean = {
    return executionOperationMap.containsKey(executionId)
  }
}
