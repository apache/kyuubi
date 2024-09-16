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

package org.apache.kyuubi.sql.compact.merge

import org.apache.spark.internal.Logging

import org.apache.kyuubi.sql.compact.UnSupportedTableException

object FileMergerFactory extends Logging {
  def create(dataSource: String, codec: Option[String]): AbstractFileMerger = {
    (dataSource, codec) match {
      case ("parquet", _) =>
        new ParquetFileMerger(dataSource, codec)
      case ("text", None) =>
        new PlainFileLikeMerger(dataSource, codec)
      case ("text", Some("gzip")) =>
        new PlainFileLikeMerger(dataSource, codec)
      case ("text", Some("bzip2")) =>
        new PlainFileLikeMerger(dataSource, codec)
      case ("csv", _) =>
        new PlainFileLikeMerger(dataSource, codec)
      case ("json", _) =>
        new PlainFileLikeMerger(dataSource, codec)
      case ("avro", _) =>
        new AvroFileMerger(dataSource, codec)
      case ("orc", _) =>
        new OrcFileMerger(dataSource, codec)
      case other =>
        throw UnSupportedTableException(s"compact table doesn't support this format $other")
    }
  }
}