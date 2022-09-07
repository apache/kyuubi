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

package org.apache.kyuubi.spark.connector.hive.read

import java.io.{FileNotFoundException, IOException}

import org.apache.parquet.io.ParquetDecodingException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.inputFileBlockHolder
import org.apache.spark.sql.internal.SQLConf

// scalastyle:off line.size.limit
// copy from https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FilePartitionReader.scala
// scalastyle:on line.size.limit
class FilePartitionReader[T](readers: Iterator[HivePartitionedFileReader[T]])
  extends PartitionReader[T] with Logging {
  private var currentReader: HivePartitionedFileReader[T] = null

  private val sqlConf = SQLConf.get
  private def ignoreMissingFiles = sqlConf.ignoreMissingFiles
  private def ignoreCorruptFiles = sqlConf.ignoreCorruptFiles

  override def next(): Boolean = {
    if (currentReader == null) {
      if (readers.hasNext) {
        try {
          currentReader = getNextReader()
        } catch {
          case e: FileNotFoundException if ignoreMissingFiles =>
            logWarning(s"Skipped missing file.", e)
            currentReader = null
          // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
          case e: FileNotFoundException if !ignoreMissingFiles =>
            throw fileNotFoundError(e)
          case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
            logWarning(
              s"Skipped the rest of the content in the corrupted file.",
              e)
            currentReader = null
        }
      } else {
        return false
      }
    }

    // In PartitionReader.next(), the current reader proceeds to next record.
    // It might throw RuntimeException/IOException and Spark should handle these exceptions.
    val hasNext =
      try {
        currentReader != null && currentReader.next()
      } catch {
        case e @ (_: SchemaColumnConvertNotSupportedException
            | _: ParquetDecodingException) =>
          throw e
        case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
          logWarning(
            s"Skipped the rest of the content in the corrupted file: $currentReader",
            e)
          false
      }
    if (hasNext) {
      true
    } else {
      close()
      currentReader = null
      next()
    }
  }

  override def get(): T = currentReader.get()

  override def close(): Unit = {
    if (currentReader != null) {
      currentReader.close()
    }
    inputFileBlockHolder.unset()
  }

  private def getNextReader(): HivePartitionedFileReader[T] = {
    val reader = readers.next()
    logInfo(s"Reading file $reader")
    // Sets InputFileBlockHolder for the file block's information
    val file = reader.file
    inputFileBlockHolder.set(file.filePath, file.start, file.length)
    reader
  }

  def fileNotFoundError(e: FileNotFoundException): Throwable = {
    new FileNotFoundException(
      e.getMessage + "\n" +
        "It is possible the underlying files have been updated. " +
        "You can explicitly invalidate the cache in Spark by " +
        "recreating the Dataset/DataFrame involved.")
  }
}
