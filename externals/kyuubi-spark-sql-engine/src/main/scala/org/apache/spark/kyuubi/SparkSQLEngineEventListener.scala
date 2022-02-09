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

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.status.{ElementTrackingStore, KVUtils}

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_UI_SESSION_LIMIT, ENGINE_UI_STATEMENT_LIMIT}
import org.apache.kyuubi.engine.spark.events.{SessionEvent, SparkOperationEvent}

class SparkSQLEngineEventListener(
    kvstore: ElementTrackingStore,
    kyuubiConf: KyuubiConf) extends SparkListener {

  /**
   * The number of SQL client sessions kept in the Kyuubi Query Engine web UI.
   */
  private val retainedSessions: Int = kyuubiConf.get(ENGINE_UI_SESSION_LIMIT)

  /**
   * The number of statements kept in the Kyuubi Query Engine web UI.
   */
  private val retainedStatements: Int = kyuubiConf.get(ENGINE_UI_STATEMENT_LIMIT)

  kvstore.addTrigger(classOf[SessionEvent], retainedSessions) { count =>
    cleanupSession(count)
  }

  kvstore.addTrigger(classOf[SparkOperationEvent], retainedStatements) { count =>
    cleanupOperation(count)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SessionEvent => updateSessionStore(e)
      case e: SparkOperationEvent => updateStatementStore(e)
      case _ => // Ignore
    }
  }

  private def updateSessionStore(event: SessionEvent): Unit = {
    kvstore.write(event, event.endTime == -1L)
  }

  private def updateStatementStore(event: SparkOperationEvent): Unit = {
    kvstore.write(event, true)
  }

  private def cleanupSession(count: Long): Unit = {
    val countToDelete = calculateNumberToRemove(count, retainedSessions)
    if (countToDelete <= 0L) {
      return
    }
    val view = kvstore.view(classOf[SessionEvent]).index("endTime").first(0L)
    val toDelete = KVUtils.viewToSeq(view, countToDelete.toInt) { j =>
      j.endTime != 0L
    }

    toDelete.foreach { j => kvstore.delete(j.getClass, j.sessionId) }
  }

  private def cleanupOperation(count: Long): Unit = {
    val countToDelete = calculateNumberToRemove(count, retainedStatements)
    if (countToDelete <= 0L) {
      return
    }
    val view = kvstore.view(classOf[SparkOperationEvent]).index("completeTime").first(0L)
    val toDelete = KVUtils.viewToSeq(view, countToDelete.toInt) { j =>
      j.completeTime != 0
    }
    toDelete.foreach { j => kvstore.delete(j.getClass, j.statementId) }
  }

  private def calculateNumberToRemove(dataSize: Long, retainedSize: Long): Long = {
    if (dataSize > retainedSize) {
      math.max(retainedSize / 10L, dataSize - retainedSize)
    } else {
      0L
    }
  }

}
