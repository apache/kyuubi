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

package org.apache.kyuubi.sql

import java.lang.reflect.Method

import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.{FileMetaData, GlobalMetaData}

object ParquetFileWriterWrapper {
  // Caused by: java.lang.IllegalAccessError: tried to access method
  // org.apache.parquet.hadoop.ParquetFileWriter.mergeInto(
  // Lorg/apache/parquet/hadoop/metadata/FileMetaData;
  // Lorg/apache/parquet/hadoop/metadata/GlobalMetaData;Z)
  // Lorg/apache/parquet/hadoop/metadata/GlobalMetaData;
  // from class org.apache.parquet.hadoop.ParquetFileWriterWrapper$

  val mergeInfoField: Method = classOf[ParquetFileWriter]
    .getDeclaredMethod(
      "mergeInto",
      classOf[FileMetaData],
      classOf[GlobalMetaData],
      classOf[Boolean])

  mergeInfoField.setAccessible(true)

  def mergeInto(
                 toMerge: FileMetaData,
                 mergedMetadata: GlobalMetaData,
                 strict: Boolean): GlobalMetaData = {
    mergeInfoField.invoke(
      null,
      toMerge.asInstanceOf[AnyRef],
      mergedMetadata.asInstanceOf[AnyRef],
      strict.asInstanceOf[AnyRef]).asInstanceOf[GlobalMetaData]
  }
}
