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

package org.apache.kyuubi.plugin.spark.authz.util

object HudiUtils {
  private val COMMIT_TIME_METADATA_FIELD = "_hoodie_commit_time"
  private val COMMIT_SEQNO_METADATA_FIELD = "_hoodie_commit_seqno"
  private val RECORD_KEY_METADATA_FIELD = "_hoodie_record_key"
  private val PARTITION_PATH_METADATA_FIELD = "_hoodie_partition_path"
  private val FILENAME_METADATA_FIELD = "_hoodie_file_name"

  private val metaFields = Set(
    COMMIT_TIME_METADATA_FIELD,
    COMMIT_SEQNO_METADATA_FIELD,
    RECORD_KEY_METADATA_FIELD,
    PARTITION_PATH_METADATA_FIELD,
    FILENAME_METADATA_FIELD)

  def isHudiMetaField(name: String): Boolean = {
    metaFields.contains(name)
  }
}
