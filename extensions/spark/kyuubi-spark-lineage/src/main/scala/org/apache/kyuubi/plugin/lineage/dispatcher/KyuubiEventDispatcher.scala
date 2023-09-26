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

package org.apache.kyuubi.plugin.lineage.dispatcher

import org.apache.spark.sql.execution.QueryExecution

import org.apache.kyuubi.Utils
import org.apache.kyuubi.events.{EventBus, KyuubiEvent}
import org.apache.kyuubi.plugin.lineage.{Lineage, LineageDispatcher}

class KyuubiEventDispatcher extends LineageDispatcher {

  override def send(qe: QueryExecution, lineage: Option[Lineage]): Unit = {
    val event = OperationLineageKyuubiEvent(qe.id, System.currentTimeMillis(), lineage, None)
    EventBus.post(event)
  }

  override def onFailure(qe: QueryExecution, exception: Exception): Unit = {
    val event =
      OperationLineageKyuubiEvent(qe.id, System.currentTimeMillis(), None, Some(exception))
    EventBus.post(event)
  }

}

case class OperationLineageKyuubiEvent(
    executionId: Long,
    eventTime: Long,
    lineage: Option[Lineage],
    exception: Option[Throwable]) extends KyuubiEvent {
  override def partitions: Seq[(String, String)] =
    ("day", Utils.getDateFromTimestamp(eventTime)) :: Nil
}
