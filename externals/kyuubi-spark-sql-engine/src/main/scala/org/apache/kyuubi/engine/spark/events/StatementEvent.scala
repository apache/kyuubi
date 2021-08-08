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

package org.apache.kyuubi.engine.spark.events

import java.util.Date

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

/**
 *
 * @param statementId
 * @param statement
 * @param appId
 * @param sessionId
 * @param state: store each state that the sql has
 * @param stateTime: the time that the sql's state change
 * @param queryExecution: contains logicPlan and physicalPlan
 * @param exeception: caught exeception if have
 */
case class StatementEvent(
    statementId: String,
    statement: String,
    appId: String,
    sessionId: String,
    var state: String,
    var stateTime: Long,
    var queryExecution: String = "",
    var exeception: String = "") extends KyuubiEvent {

  override def eventType: String = "statement"

  override def schema: StructType = Encoders.product[StatementEvent].schema

  override def toJson: String = JsonProtocol.productToJson(this)

  override def toString: String = {
    s"""
       |    Statement application ID: $appId
       |              statement:  $statement
       |              statement ID: $statementId
       |              session ID: $sessionId
       |    State time: ${new Date(stateTime)}
       |    State: $state
       |    ${if (queryExecution.nonEmpty) "QueryExecution: " + queryExecution else ""}
       |    ${if (exeception.nonEmpty) "Exeception: " + exeception else ""}""".stripMargin
  }
}
