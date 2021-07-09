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

package org.apache.spark.kyuubi.entity

import scala.collection.mutable.Map

/**
 * This object store the summary infomation about statement.
 * You can use statementId to get all jobs' or stages' metric that this statement has.
 * @param statementId
 * @param statement
 * @param appId
 * @param sessionId
 * @param physicalPlan
 * @param sparkUser
 * @param exception
 * @param stateTimeMap
 */
case class KyuubiStatementInfo(
    statementId: String,
    statement: String,
    appId: String,
    sessionId: String,
    var physicalPlan: String,
    sparkUser: String,
    var exception: String,
    // This map store the statement's every state and the time of occurrence
    var stateTimeMap: Map[String, Long])
