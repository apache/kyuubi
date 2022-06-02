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

import java.io.Closeable

import org.apache.kyuubi.server.statestore.api.SessionMetadata
import org.apache.kyuubi.session.SessionType.SessionType

trait StateStore extends Closeable {

  /**
   * Insert a metadata into state store.
   */
  def insertMetadata(metadata: SessionMetadata): Unit

  /**
   * Get the persisted metadata by batch identifier.
   * @param identifier the identifier.
   * @param stateOnly only return the state related column values.
   * @return selected metadata.
   */
  def getMetadata(identifier: String, stateOnly: Boolean): SessionMetadata

  /**
   * Get the metadata list with filter conditions, offset and size.
   * @param sessionType the session type.
   * @param engineType the engine type.
   * @param userName the user name.
   * @param state the state.
   * @param kyuubiInstance the kyuubi instance.
   * @param from the batch offset.
   * @param size the batch size to get.
   * @param stateOnly only return the state related column values.
   * @return selected metadata list.
   */
  def getMetadataList(
      sessionType: SessionType,
      engineType: String,
      userName: String,
      state: String,
      kyuubiInstance: String,
      from: Int,
      size: Int,
      stateOnly: Boolean): Seq[SessionMetadata]

  /**
   * Update the metadata according to identifier.
   * Note that, it will only update the state and engine related metadata.
   */
  def updateMetadata(metadata: SessionMetadata): Unit

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
