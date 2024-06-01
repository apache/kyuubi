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

package org.apache.kyuubi.engine.spark.operation

import scala.Array._
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocatedFileStatus, Path}
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce.OrcInputFormat
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources.RecordReaderIterator
import org.apache.spark.sql.execution.datasources.orc.OrcDeserializer
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.operation.{FetchIterator, IterableFetchIterator}

class FetchOrcStatement(spark: SparkSession) {

  var orcIter: OrcFileIterator = _
  def getIterator(path: String, orcSchema: StructType): FetchIterator[Row] = {
    val conf = spark.sparkContext.hadoopConfiguration
    val savePath = new Path(path)
    val fsIterator = savePath.getFileSystem(conf).listFiles(savePath, false)
    val list = new ListBuffer[LocatedFileStatus]
    while (fsIterator.hasNext) {
      val file = fsIterator.next()
      if (file.getPath.getName.endsWith(".orc") && file.getLen > 0) {
        list += file
      }
    }
    val toRowConverter: InternalRow => Row = {
      CatalystTypeConverters.createToScalaConverter(orcSchema)
        .asInstanceOf[InternalRow => Row]
    }
    val colId = range(0, orcSchema.size)
    val fullSchema = orcSchema.map(f =>
      AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
    val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)
    val deserializer = new OrcDeserializer(orcSchema, colId)
    orcIter = new OrcFileIterator(list)
    val iterRow = orcIter.map(value =>
      unsafeProjection(deserializer.deserialize(value)))
      .map(value => toRowConverter(value))
    new IterableFetchIterator[Row](new Iterable[Row] {
      override def iterator: Iterator[Row] = iterRow
    })
  }

  def close(): Unit = {
    orcIter.close()
  }
}

class OrcFileIterator(fileList: ListBuffer[LocatedFileStatus]) extends Iterator[OrcStruct] {

  private val iters = fileList.map(x => getOrcFileIterator(x))

  var idx = 0

  override def hasNext: Boolean = {
    if (idx >= iters.size) return false
    val hasNext = iters(idx).hasNext
    if (!hasNext) {
      iters(idx).close()
      idx += 1
      // skip empty file
      while (idx < iters.size) {
        if (iters(idx).hasNext) {
          return true
        } else {
          iters(idx).close()
          idx = idx + 1
        }
      }
    }
    hasNext
  }

  override def next(): OrcStruct = {
    iters(idx).next()
  }

  def close(): Unit = {
    iters.foreach(_.close())
  }

  private def getOrcFileIterator(file: LocatedFileStatus): RecordReaderIterator[OrcStruct] = {
    val orcRecordReader = {
      val split =
        new FileSplit(file.getPath, 0, file.getLen, Array.empty[String])
      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val hadoopAttemptContext =
        new TaskAttemptContextImpl(new Configuration(), attemptId)
      val oif = new OrcInputFormat[OrcStruct]
      oif.createRecordReader(split, hadoopAttemptContext)
    }
    new RecordReaderIterator[OrcStruct](orcRecordReader)
  }
}
