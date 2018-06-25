/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.ui

import java.util.{Properties, UUID}

import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkFunSuite}
import org.apache.spark.scheduler.SparkListenerJobStart

class KyuubiServerListenerSuite extends SparkFunSuite {

  test("kyuubi server listener") {
    val conf = new SparkConf(loadDefaults = true)
    val li = new KyuubiServerListener(conf)
    assert(li.getOnlineSessionNum === 0)
    assert(li.getTotalRunning === 0)
    assert(li.getSessionList.isEmpty)
    assert(li.getExecutionList.isEmpty)

    val ip = KyuubiSparkUtil.localHostName()
    val sessionId = UUID.randomUUID().toString
    val user = KyuubiSparkUtil.getCurrentUserName()

    li.onSessionCreated(ip, sessionId, user)
    assert(li.getSessionList.nonEmpty)
    assert(li.getSession(sessionId).nonEmpty)
    assert(li.getSession(sessionId).get.userName === user)
    assert(li.getOnlineSessionNum === 1)
    li.onSessionClosed(sessionId)
    assert(li.getSessionList.nonEmpty)
    assert(li.getSession(sessionId).nonEmpty)
    assert(li.getSession(sessionId).get.userName === user)
    assert(li.getSession(sessionId).get.finishTimestamp !== 0)
    assert(li.getOnlineSessionNum === 0)

    val statement = "show tables"
    val groupId = UUID.randomUUID().toString
    val id = groupId
    li.onStatementStart(id, sessionId, statement, groupId, user)
    assert(li.getExecutionList.nonEmpty)
    assert(li.getSession(sessionId).get.totalExecution === 1)
    assert(li.getExecutionList.head.groupId === groupId)
    assert(li.getExecutionList.head.state === ExecutionState.STARTED)
    assert(li.getExecutionList.head.sessionId === sessionId)
    assert(li.getTotalRunning === 1)

    val plan = "nothing else"
    li.onStatementParsed(id, plan)
    assert(li.getExecutionList.head.executePlan === plan)
    assert(li.getExecutionList.head.state === ExecutionState.COMPILED)

    val err = "nothing err"
    Thread.sleep(1000)
    li.onStatementError(id, err, err)
    val finishTimestamp1 = li.getExecutionList.head.finishTimestamp
    assert(finishTimestamp1 !== 0)
    assert(li.getExecutionList.head.state === ExecutionState.FAILED)
    assert(li.getExecutionList.head.detail === err)
    assert(li.getTotalRunning === 0)

    val props = new Properties()
    props.setProperty(KyuubiSparkUtil.getJobGroupIDKey(), groupId)
    val jobStart = new SparkListenerJobStart(1, System.currentTimeMillis(), Seq.empty, props)
    li.onJobStart(jobStart)

    assert(li.getExecutionList.head.jobId.head === "1")
    assert(li.getExecutionList.head.groupId === groupId)

    li.onStatementFinish(id)
    assert(li.getExecutionList.head.finishTimestamp !== finishTimestamp1)
    assert(li.getExecutionList.head.state === ExecutionState.FINISHED)
    assert(li.getTotalRunning === -1)
  }
}
