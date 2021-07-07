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

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.kyuubi.entity.entity.KStatement

object SparkSQLMetrics {

  private final val executionOperationMap = new ConcurrentHashMap[Long, String]()

  private final val operationStatementMap = new ConcurrentHashMap[String, KStatement]()

  private final val executionPhysicalPlanMap = new java.util.HashMap[Long, String]()

  def addPhysicalPlanForExecution(executionId: Long, physicalPlan: String): Unit = {
    executionPhysicalPlanMap.putIfAbsent(executionId, physicalPlan)
  }

  def addExecutionAndOperationRelation(executionId: Long, operationId: String): Unit = {
//    if (executionOperationMap.containsKey(executionId)
//      && executionOperationMap.get(executionId).equals("empty")) {
//      executionOperationMap.put(executionId, operationId)
//    }
    executionOperationMap.putIfAbsent(executionId, operationId)
  }

  def addStatementDetailForOperation(operationId: String, kStatement: KStatement): Unit = {
    operationStatementMap.putIfAbsent(operationId, kStatement)
  }

}
