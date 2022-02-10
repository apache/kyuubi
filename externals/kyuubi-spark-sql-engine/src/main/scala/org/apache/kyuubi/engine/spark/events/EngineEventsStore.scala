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

import scala.collection.JavaConverters._

import org.apache.spark.util.kvstore.KVStore

/**
 * A memory store that tracking the number of statements and sessions, it provides:
 *
 * - stores all events.
 * - cleanup the events when reach a certain threshold:
 * 1). remove the finished events first.
 * 2). remove the active events if still reach the threshold.
 */
class EngineEventsStore(store: KVStore) {

  /**
   * get all session events order by startTime
   */
  def getSessionList: Seq[SessionEvent] = {
    store.view(classOf[SessionEvent]).asScala.toSeq
  }

  def getSession(sessionId: String): Option[SessionEvent] = {
    try {
      Some(store.read(classOf[SessionEvent], sessionId))
    } catch {
      case _: NoSuchElementException => None
    }
  }

  /**
   * get all statement events order by startTime
   */
  def getStatementList: Seq[SparkOperationEvent] = {
    store.view(classOf[SparkOperationEvent]).asScala.toSeq
  }

  def getStatement(statementId: String): Option[SparkOperationEvent] = {
    try {
      Some(store.read(classOf[SparkOperationEvent], statementId))
    } catch {
      case _: NoSuchElementException => None
    }
  }

  def getSessionCount: Long = {
    store.count(classOf[SessionEvent])
  }

  def getStatementCount: Long = {
    store.count(classOf[SparkOperationEvent])
  }

}
