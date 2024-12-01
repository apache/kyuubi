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

package org.apache.kyuubi.server.metadata

import java.io.Closeable

import org.apache.kyuubi.server.metadata.api.{Metadata, MetadataFilter}

trait MetadataStore extends Closeable {

  /**
   * Insert a metadata into metadata store.
   */
  def insertMetadata(metadata: Metadata): Unit

  /**
   * Find unscheduled batch job metadata and pick up it to submit.
   * @param kyuubiInstance the Kyuubi instance picked batch job
   * @return selected metadata for submitting or None if no sufficient items
   */
  def pickMetadata(kyuubiInstance: String): Option[Metadata]

  /**
   * Transfer state of metadata from the existing state to another
   * @param identifier the identifier.
   * @param fromState the desired current state
   * @param targetState the desired target state
   * @return `true` if the metadata state was same as `fromState`, and successfully
   *         transitioned to `targetState`, otherwise `false` is returned
   */
  def transformMetadataState(identifier: String, fromState: String, targetState: String): Boolean

  /**
   * Get the persisted metadata by batch identifier.
   * @param identifier the identifier.
   * @return selected metadata.
   */
  def getMetadata(identifier: String): Metadata

  /**
   * Get the metadata list with filter conditions, offset and size.
   * @param filter the metadata filter conditions.
   * @param from the metadata offset.
   * @param size the size to get.
   * @param desc the order of metadata list.
   * @param orderByKeyId the result order by auto increment key_id, which is stable but might slow.
   *                     If false, order by create_time.
   * @return selected metadata list.
   */
  def getMetadataList(
      filter: MetadataFilter,
      from: Int,
      size: Int,
      desc: Boolean = false,
      orderByKeyId: Boolean = true): Seq[Metadata]

  /**
   * Count the metadata list with filter conditions.
   * @param filter the metadata filter conditions.
   * @return the count of metadata satisfied the filter condition.
   */
  def countMetadata(filter: MetadataFilter): Int

  /**
   * Update the metadata according to identifier.
   * Note that, it will only update the state and engine related metadata.
   */
  def updateMetadata(metadata: Metadata): Unit

  /**
   * Cleanup meta data by identifier.
   */
  def cleanupMetadataByIdentifier(identifier: String): Unit

  /**
   * Check and cleanup the terminated batches information with maxAge limitation.
   * @param maxAge the batch state info maximum age.
   */
  def cleanupMetadataByAge(maxAge: Long): Unit
}
