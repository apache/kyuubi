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
import org.apache.spark.sql.internal.SQLConf
import org.mockito.Mockito.when
import org.scalatest.mock.MockitoSugar

class KyuubiServerListenerSuite extends SparkFunSuite with MockitoSugar{
  val ip = KyuubiSparkUtil.localHostName()

  test("kyuubi server listener") {
    val conf = new SparkConf(loadDefaults = true)
    val li = new KyuubiServerListener(conf)
    assert(li.getOnlineSessionNum === 0)
    assert(li.getTotalRunning === 0)
    assert(li.getSessionList.isEmpty)
    assert(li.getExecutionList.isEmpty)

    val sessionId = UUID.randomUUID().toString
    val user = KyuubiSparkUtil.getCurrentUserName

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
    props.setProperty(KyuubiSparkUtil.getJobGroupIDKey, groupId)
    val jobStart = new SparkListenerJobStart(1, System.currentTimeMillis(), Seq.empty, props)
    li.onJobStart(jobStart)

    assert(li.getExecutionList.head.jobId.head === "1")
    assert(li.getExecutionList.head.groupId === groupId)

    Thread.sleep(1000)
    li.onStatementFinish(id)
    assert(li.getExecutionList.head.finishTimestamp !== finishTimestamp1)
    assert(li.getExecutionList.head.state === ExecutionState.FINISHED)
    assert(li.getTotalRunning === -1)
  }

  test("on job start") {
    val jobStart = mock[SparkListenerJobStart]

    val props = new Properties()
    val sessionId = UUID.randomUUID().toString
    val statementId = UUID.randomUUID().toString
    props.setProperty(KyuubiSparkUtil.getJobGroupIDKey, statementId)
    when(jobStart.properties).thenReturn(props)
    when(jobStart.jobId).thenReturn(1)
    val conf = new SparkConf()
    val li = new KyuubiServerListener(conf)
    li.onSessionCreated(ip, sessionId)
    assert(li.getSession(sessionId).get.userName === "UNKNOWN")
    li.onStatementStart(statementId, sessionId, "show tables", statementId)
    assert(li.getExecutionList.head.groupId === statementId)
    li.onJobStart(jobStart)
    assert(li.getExecutionList.head.jobId.contains("1"))

    val jobStart2 = mock[SparkListenerJobStart]
    when(jobStart2.properties).thenReturn(null)
    when(jobStart2.jobId).thenReturn(2)
    li.onJobStart(jobStart2)
    assert(!li.getExecutionList.head.jobId.contains("2"))

    val jobStart3 = mock[SparkListenerJobStart]
    when(jobStart3.properties).thenReturn(new Properties())
    when(jobStart3.jobId).thenReturn(3)
    li.onJobStart(jobStart3)
    assert(!li.getExecutionList.head.jobId.contains("3"))
  }

  test("trim session if necessary") {
    val conf = new SparkConf().set(SQLConf.THRIFTSERVER_UI_SESSION_LIMIT.key, "1")
    val li = new KyuubiServerListener(conf)
    val sessionId = UUID.randomUUID().toString
    li.onSessionCreated(ip, sessionId)
    val sessionId2 = UUID.randomUUID().toString
    li.onSessionCreated(ip, sessionId2)
    val sessionId3 = UUID.randomUUID().toString
    li.onSessionCreated(ip, sessionId3)
    // trim nothing when finish time 0
    assert(li.getSessionList.forall(_.finishTimestamp === 0L))
    assert(li.getSessionList.size === 3)

    // trim id 1
    li.onSessionClosed(sessionId)
    assert(li.getSessionList.forall(_.finishTimestamp === 0L))
    assert(li.getSessionList.size === 2)
    // trim id 2
    li.onSessionClosed(sessionId2)
    assert(li.getSessionList.size === 1)
    // remain id 3, when lower bound meets
    li.onSessionClosed(sessionId3)
    assert(li.getSessionList.size === 1)
    assert(li.getSessionList.forall(_.finishTimestamp !== 0L))

    // trim id 3
    val sessionId4 = UUID.randomUUID().toString
    li.onSessionCreated(ip, sessionId4)
    assert(li.getSessionList.size === 1)
    assert(li.getSessionList.forall(_.finishTimestamp === 0L))


    conf.set(SQLConf.THRIFTSERVER_UI_SESSION_LIMIT.key, "20")
    val li2 = new KyuubiServerListener(conf)
    (0 until 30).foreach(p => li2.onSessionCreated(ip, p.toString))
    li2.getSessionList.take(5).foreach(_.finishTimestamp = 1)
    li2.onSessionClosed("29")
    assert(li2.getSessionList.size === 28)
    li2.onSessionClosed("28")
    assert(li2.getSessionList.size === 26)

  }

  test("trim execution if necessary") {
    val conf = new SparkConf().set(SQLConf.THRIFTSERVER_UI_STATEMENT_LIMIT.key, "1")
    val li = new KyuubiServerListener(conf)
    val sessionId = UUID.randomUUID().toString
    li.onSessionCreated(ip, sessionId)
    val statementId = UUID.randomUUID().toString
    val statement = "show tables"
    li.onStatementStart(statementId, sessionId, statement, statementId)
    assert(li.getExecutionList.size === 1)
    assert(li.getExecutionList.forall(_.finishTimestamp === 0L))
    val statementId2 = UUID.randomUUID().toString
    li.onStatementStart(statementId2, sessionId, statement, statementId2)
    assert(li.getExecutionList.size === 2)
    assert(li.getExecutionList.forall(_.finishTimestamp === 0L))
    val statementId3 = UUID.randomUUID().toString
    li.onStatementStart(statementId3, sessionId, statement, statementId3)
    assert(li.getExecutionList.size === 3)
    assert(li.getExecutionList.forall(_.finishTimestamp === 0L))

    // trim id 1
    li.onStatementFinish(statementId)
    assert(li.getExecutionList.size === 2)
    assert(li.getExecutionList.forall(_.finishTimestamp === 0L))
    // trim id 2
    li.onStatementFinish(statementId2)
    assert(li.getExecutionList.size === 1)
    assert(li.getExecutionList.forall(_.finishTimestamp === 0L))
    // remain id 3, when lower bound meets
    li.onStatementFinish(statementId3)
    assert(li.getExecutionList.size === 1)
    assert(li.getExecutionList.forall(_.finishTimestamp !== 0L))
    // trim id 3
    val statementId4 = UUID.randomUUID().toString
    li.onStatementStart(statementId4, sessionId, statement, statementId4)
    assert(li.getExecutionList.size === 1)
    assert(li.getExecutionList.forall(_.finishTimestamp === 0L))

    conf.set(SQLConf.THRIFTSERVER_UI_STATEMENT_LIMIT.key, "20")
    val li2 = new KyuubiServerListener(conf)
    li2.onSessionCreated(ip, sessionId)

    (0 until 30).foreach { id =>
      li2.onStatementStart(id.toString, sessionId, statement, id.toString)
    }
    li2.getExecutionList.take(5).foreach(_.finishTimestamp = 1)
    // trim 2 statement
    li2.onStatementFinish("29")
    assert(li2.getExecutionList.size === 28)

    li2.onStatementFinish("28")
    assert(li2.getExecutionList.size === 26)

  }
}
