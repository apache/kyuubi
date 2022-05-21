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

package org.apache.kyuubi.server.statestore

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.statestore.api._

class InMemoryStateStore(conf: KyuubiConf) extends StateStore {
  private val batchIdToBatch = new ConcurrentHashMap[String, BatchState]().asScala
  private val closedBatches = new ConcurrentHashMap[String, BatchState]().asScala
  private val stateMaxAge = conf.get(KyuubiConf.SERVER_STATE_STORE_MAX_AGE)
  private val stateMaxNumber = conf.get(KyuubiConf.SERVER_STATE_STORE_MAX_NUMBER)

  override def shutdown(): Unit = {
    batchIdToBatch.clear()
    closedBatches.clear()
  }

  override def createBatch(batch: BatchState): Unit = {
    batchIdToBatch.put(batch.id, batch)
  }

  override def getBatch(batchId: String): BatchState = {
    batchIdToBatch.get(batchId).orElse(closedBatches.get(batchId)).orNull
  }

  override def updateBatchAppInfo(
      batchId: String,
      appId: String,
      appName: String,
      appUrl: String,
      appState: String,
      appError: Option[String]): Unit = {
    batchIdToBatch.get(batchId).map { batch =>
      val updatedBatch = batch.copy(
        appId = appId,
        appName = appName,
        appUrl = appUrl,
        appState = appState,
        appError = appError)
      batchIdToBatch.put(batchId, updatedBatch)
    }.getOrElse(throw new NoSuchElementException(s"No batch for $batchId"))
  }

  override def closeBatch(batchId: String, state: String, endTime: Long): Unit = {
    batchIdToBatch.remove(batchId).map { batch =>
      val closedBatch = batch.copy(state = state, endTime = endTime)
      closedBatches.put(batchId, closedBatch)
    }
  }

  override def getBatches(
      batchType: String,
      batchOwner: String,
      batchState: String,
      from: Int,
      size: Int): Seq[BatchState] = {
    batchIdToBatch.values.toSeq.union(closedBatches.values.toSeq).sortBy(_.id).filter { batch =>
      Option(batchType).filter(_.nonEmpty).forall(_.equalsIgnoreCase(batch.batchType)) &&
      Option(batchOwner).filter(_.nonEmpty).forall(_.equals(batch.batchOwner)) &&
      Option(batchState).filter(_.nonEmpty).forall(_.equalsIgnoreCase(batch.state))
    }.slice(from, from + size)
  }

  /**
   * For in-memory state store, it does not support batches recovery.
   */
  override def getBatchesToRecover(
      kyuubiInstance: String,
      from: Int,
      size: Int): Seq[BatchState] = {
    Seq.empty
  }

  /**
   * For in-memory state store, it does not save batch request for recovery.
   */
  override def saveBatchMeta(batchRequest: BatchMeta): Unit = {}

  /**
   * For in-memory state store, it does not get batch request for recovery.
   */
  override def getBatchMeta(batchId: String): BatchMeta = {
    null
  }

  override def checkAndCleanupBatches(): Unit = {
    val maxTime = System.currentTimeMillis() - stateMaxAge
    val sortedFinishedBatches = closedBatches.values.toSeq.sortBy(_.endTime)

    val reservedIndexByTime = sortedFinishedBatches.indexWhere(_.endTime > maxTime)
    val reservedIndexByNum = sortedFinishedBatches.size - stateMaxNumber
    val reservedIndex = math.max(reservedIndexByTime, reservedIndexByNum)
    sortedFinishedBatches.slice(0, reservedIndex).foreach { batch =>
      closedBatches.remove(batch.id)
    }
  }

  override def cleanupBatch(batchId: String): Unit = {
    batchIdToBatch.remove(batchId).orElse(closedBatches.remove(batchId))
  }
}
