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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_UI_SESSION_LIMIT

/**
 * A memory store that tracking the number of statements and sessions, it provides:
 *
 * - stores all events.
 * - cleanup the events when reach a certain threshold:
 * 1). remove the finished events first.
 * 2). remove the active events if still reach the threshold.
 *
 * // TODO KYUUBI #983 this store will be used in the third task.
 */
class EngineEventsStore(conf: KyuubiConf) {

  /**
   * The number of SQL client sessions kept in the Kyuubi Query Engine web UI.
   */
  private val retainedSessions: Int = conf.get(ENGINE_UI_SESSION_LIMIT)

  /**
   * store all session events.
   */
  val sessions = new ConcurrentHashMap[String, SessionEvent]

  /**
   * get all session events order by startTime
   */
  def getSessionList: Seq[SessionEvent] = {
    sessions.values().asScala.toSeq.sortBy(_.startTime)
  }

  def getSession(sessionId: String): Option[SessionEvent] = {
    Option(sessions.get(sessionId))
  }

  /**
   * save session events and check the capacity threshold
   */
  def saveSession(sessionEvent: SessionEvent): Unit = {
    sessions.put(sessionEvent.sessionId, sessionEvent)
    checkSessionCapacity()
  }

  /**
   * cleanup the session events if reach the threshold
   */
  private def checkSessionCapacity(): Unit = {
    var countToDelete = sessions.size - retainedSessions

    val reverseSeq = sessions.values().asScala.toSeq.sortBy(_.startTime).reverse

    // remove finished sessions first.
    for (event <- reverseSeq if event.endTime != 0L && countToDelete > 0) {
      sessions.remove(event.sessionId)
      countToDelete -= 1
    }

    // remove active event if still reach the threshold
    for (event <- reverseSeq if countToDelete > 0) {
      sessions.remove(event.sessionId)
      countToDelete -= 1
    }
  }
}

