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

package org.apache.kyuubi.engine.spark.monitor

import java.util.concurrent.ArrayBlockingQueue

import org.apache.kyuubi.engine.spark.monitor.entity.{KyuubiInfo, KyuubiStatementInfo}

object KyuubiStatementMonitor {

  /**
   * This blockingQueue store kyuubiStatementInfo.
   *
   * Notice:
   *    1. When we remove items from this queue, we should ensure those statements have finished.
   *       If not, we should put them into this queue again.
   *    2. There have two kinds of threshold to trigger when to remove items from this queue:
   *      a. time
   *      b. this queue's current size
   */
  // TODO: Capacity should make configurable
  private val kyuubiStatementQueue = new ArrayBlockingQueue[KyuubiStatementInfo](10)

  /**
   * This function is used for putting kyuubiStatementInfo into blockingQueue(statementQueue).
   * Every time we put an item into this queue, we should judge this queue's current size at first.
   * If the size is less than threshold, we need to remove items from this queue.
   * @param kyuubiStatementInfo
   */
  // TODO: Lack size type threshold and time type threshold
  def putStatementInfoIntoQueue(kyuubiStatementInfo: KyuubiStatementInfo): Unit = {
    if (kyuubiStatementQueue.size() >= 7) {
      removeAndDumpStatementInfoFromQueue()
    }
    kyuubiStatementQueue.put(kyuubiStatementInfo)
  }

  /**
   * This function is used for removing kyuubiStatementInfo from blockingQueue(statementQueue)
   * and dumpping them to a file by threshold.
   */
  // TODO: Need ensure those items have finished. If not, we should put them into this queue again.
  def removeAndDumpStatementInfoFromQueue(): Unit = {
    // TODO: Just for test
    kyuubiStatementQueue.clear()
  }

  // TODO: For test
  def getQueueSize[T](c: Class[T]): Int = {
    if (c.equals(KyuubiStatementInfo.getClass)) {
      return kyuubiStatementQueue.size()
    } else {
      return -1
    }
  }

  // TODO: For test
  def copyQueue[T](c: Class[T]): ArrayBlockingQueue[KyuubiInfo] = {
    if (c.equals(KyuubiStatementInfo.getClass)) {
      val kyuubistatementQueueCopy = new ArrayBlockingQueue[KyuubiInfo](kyuubiStatementQueue.size())
      kyuubistatementQueueCopy.addAll(kyuubiStatementQueue)
      return kyuubistatementQueueCopy
    }
    return null
  }
}
