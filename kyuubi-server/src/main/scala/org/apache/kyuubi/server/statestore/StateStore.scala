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

import org.apache.kyuubi.server.statestore.api._

trait StateStore {

  /**
   * Create a batch.
   */
  def createBatch(batch: Batch): Unit

  /**
   * Get the persisted batch info by batch id.
   */
  def getBatch(batchId: String): Batch

  /**
   * Update the batch application info.
   * @param batchId the batch id.
   * @param appId the batch application id.
   * @param appName the batch application name.
   * @param appUrl the batch application tracking url.
   * @param appState the batch application state.
   * @param appError the batch application error for diagnose.
   */
  def updateBatchAppInfo(
      batchId: String,
      appId: String,
      appName: String,
      appUrl: String,
      appState: String,
      appError: Option[String]): Unit

  /**
   * Close a batch with final state and end time.
   * @param batchId the batch id.
   * @param state the batch final state.
   * @param endTime  the batch end time.
   */
  def closeBatch(batchId: String, state: String, endTime: Long): Unit

  /**
   * Get the batches with filter conditions, offset and size.
   * @param batchType the batch type.
   * @param batchOwner the batch owner.
   * @param batchState the batch state.
   * @param from the batch offset.
   * @param size the batch size to get.
   * @return selected batches.
   */
  def getBatches(
      batchType: String,
      batchOwner: String,
      batchState: String,
      from: Int,
      size: Int): Seq[Batch]

  /**
   * Get the batches to recover by the kyuubi instance with offset and size.
   */
  def getBatchesToRecover(kyuubiInstance: String, from: Int, size: Int): Seq[Batch]

  /**
   * Save the batch request.
   */
  def saveBatchRequest(batchRequest: BatchRequest): Unit

  /**
   * Get the batch request by batch id.
   */
  def getBatchRequest(batchId: String): BatchRequest

  /**
   * Check and cleanup the batches information with maxAge or max batches number limitation.
   * It will cleanup both batch and batch request records.
   */
  def checkAndCleanupBatches(): Unit

  /**
   * Cleanup batch and batch request by batch id.
   */
  def cleanupBatch(batchId: String): Unit
}
