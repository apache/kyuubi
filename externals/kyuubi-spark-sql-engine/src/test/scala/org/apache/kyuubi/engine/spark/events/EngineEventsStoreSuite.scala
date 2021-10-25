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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_UI_SESSION_LIMIT, ENGINE_UI_STATEMENT_LIMIT}

class EngineEventsStoreSuite extends KyuubiFunSuite {

  test("ensure that the sessions are stored in order") {
    val store = new EngineEventsStore(KyuubiConf())

    val s1 = SessionEvent("a", "ea", "test1", "1.1.1.1", 1L)
    val s2 = SessionEvent("c", "ea", "test2", "1.1.1.1", 3L)
    val s3 = SessionEvent("b", "ea", "test3", "1.1.1.1", 2L)

    store.saveSession(s1)
    store.saveSession(s2)
    store.saveSession(s3)

    assert(store.getSessionList.size == 3)
    assert(store.getSessionList.head.sessionId == "a")
    assert(store.getSessionList.last.sessionId == "c")
  }

  test("test drop sessions when reach the threshold ") {
    val conf = KyuubiConf()
    conf.set(ENGINE_UI_SESSION_LIMIT, 3)

    val store = new EngineEventsStore(conf)
    for (i <- 1 to 5) {
      val s = SessionEvent(s"b$i", "ea", s"test$i", "1.1.1.1", 2L)
      store.saveSession(s)
    }

    assert(store.getSessionList.size == 3)
  }

  test("test drop sessions when reach the threshold, and try to keep active events.") {
    val conf = KyuubiConf()
    conf.set(ENGINE_UI_SESSION_LIMIT, 3)

    val store = new EngineEventsStore(conf)

    store.saveSession(SessionEvent("s1", "ea", "test1", "1.1.1.1", 1L, -1L))
    store.saveSession(SessionEvent("s2", "ea", "test1", "1.1.1.1", 2L, -1L))
    store.saveSession(SessionEvent("s3", "ea", "test1", "1.1.1.1", 3L, 1L))
    store.saveSession(SessionEvent("s4", "ea", "test1", "1.1.1.1", 4L, -1L))

    assert(store.getSessionList.size == 3)
    assert(store.getSessionList(2).sessionId == "s4")
  }

  test("test check session after update session") {
    val store = new EngineEventsStore(KyuubiConf())
    val s = SessionEvent("abc", "ea", "test3", "1.1.1.1", 2L)
    store.saveSession(s)

    val finishTimestamp: Long = 456L
    s.endTime = finishTimestamp
    store.saveSession(s)

    assert(store.getSession("abc").get.endTime == finishTimestamp)
  }

  test("ensure that the statements are stored in order") {
    val store = new EngineEventsStore(KyuubiConf())

    val s1 = SparkStatementEvent("a", "ea1", "select 1", "app1", "sid1", 1L, "RUNNING", 2L)
    val s2 = SparkStatementEvent("c", "ea2", "select 2", "app2", "sid1", 2L, "RUNNING", 2L)
    val s3 = SparkStatementEvent("b", "ea3", "select 3", "app3", "sid1", 3L, "RUNNING", 2L)

    store.saveStatement(s1)
    store.saveStatement(s2)
    store.saveStatement(s3)

    assert(store.getStatementList.size == 3)
    assert(store.getStatementList.head.statementId == "ea1")
    assert(store.getStatementList.last.statementId == "ea3")
  }

  test("test drop statements when reach the threshold ") {
    val conf = KyuubiConf()
    conf.set(ENGINE_UI_STATEMENT_LIMIT, 3)

    val store = new EngineEventsStore(conf)
    for (i <- 1 to 5) {
      val s = SparkStatementEvent("a", s"ea1${i}", "select 1", "app1", "sid1", 1L, "RUNNING", 2L)
      store.saveStatement(s)
    }

    assert(store.getStatementList.size == 3)
  }

  test("test drop statements when reach the threshold, and try to keep active events.") {
    val conf = KyuubiConf()
    conf.set(ENGINE_UI_STATEMENT_LIMIT, 3)

    val store = new EngineEventsStore(conf)

    store.saveStatement(SparkStatementEvent("a", "s1", "select 1", "a1", "si1", 1L, "RUNNING", -1L))
    store.saveStatement(SparkStatementEvent("a", "s2", "select 1", "a2", "si1", 2L, "RUNNING", -1L))
    store.saveStatement(SparkStatementEvent("a", "s3", "1", "a3", "si1", 3L, "ERROR", 3L, 3L))
    store.saveStatement(SparkStatementEvent("a", "s4", "select 1", "a4", "si1", 4L, "RUNNING", -1L))

    assert(store.getStatementList.size == 3)
    assert(store.getStatementList(2).statementId == "s4")
  }

}
