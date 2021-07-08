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

  private val executionOperationMap = new ConcurrentHashMap[Long, String]()

  private val operationStatementMap = new ConcurrentHashMap[String, KStatement]()

  private val executionPhysicalPlanMap = new ConcurrentHashMap[Long, String]()

  // This map store each execution startTime
  private val executionStartTimeMap = new ConcurrentHashMap[Long, Long]()

  def addPhysicalPlanForExecutionId(executionId: Long, physicalPlan: String): Unit = {
    executionPhysicalPlanMap.putIfAbsent(executionId, physicalPlan)
  }

  def addPhysicalPlanIntoKs(executionId: Long, operatioId: String): Unit = {
    // Store the relationship between executionId and OperationId
    executionOperationMap.putIfAbsent(executionId, operatioId)
    // Get physicalPlan
    // TODO: 这个操作如果2次触发，但是第一次已经删除掉了，会造成第二次的数据永远不会删除，需要有判断
    val physicalPlan = executionPhysicalPlanMap.remove(executionId)
    operationStatementMap.get(operatioId).setPhysicPlan(physicalPlan)
    operationStatementMap.get(operatioId).setExecutionId(executionId)

    // Get the time that the state is RUNNING
    val startTime = executionStartTimeMap.remove(executionId)
    // Add executionStartTimeMap into ks
    operationStatementMap.get(operatioId).setStateTime(OperationState.RUNNING.toString, startTime)
  }

  def addStatementDetailForOperationId(operationId: String, kStatement: KStatement): Unit = {
    operationStatementMap.putIfAbsent(operationId, kStatement)
  }

  def addEachStateTimeForExecutionId(executionId: Long, state: String, time: Long): Unit = {
    if (state.equals(OperationState.RUNNING.toString)) {
      executionStartTimeMap.put(executionId, time)
    }
  }

  def addFinishTimeForExecutionId(executionId: Long, finishTime: Long): Unit = {
    // Get operationId
    val operationId = executionOperationMap.remove(executionId)
    operationStatementMap.get(operationId).setStateTime(OperationState.FINISHED.toString, finishTime)
  }

}
