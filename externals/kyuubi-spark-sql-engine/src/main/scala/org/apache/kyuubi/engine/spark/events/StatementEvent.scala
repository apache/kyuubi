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

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.Utils

/**
 * @param statementId: the identifier of operationHandler
 * @param statement: the sql that you execute
 * @param appId: application id a.k.a, the unique id for engine
 * @param sessionId: the identifier of a session
 * @param startTime: the start time of this statement
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
    startTime: Long,
    var state: String,
    var stateTime: Long,
    var queryExecution: String = "",
    var exeception: String = "") extends KyuubiEvent {

  override def schema: StructType = Encoders.product[StatementEvent].schema
  override def partitions: Seq[(String, String)] =
    ("day", Utils.getDateFromTimestamp(startTime)) :: Nil
}
