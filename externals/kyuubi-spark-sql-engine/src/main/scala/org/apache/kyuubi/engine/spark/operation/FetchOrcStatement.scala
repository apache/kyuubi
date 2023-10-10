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
  def getIterator(path: String, orcSchema: StructType): FetchIterator[Row] = {
    val conf = spark.sparkContext.hadoopConfiguration
    val savePath = new Path(path)
    val fsIterator = savePath.getFileSystem(conf).listFiles(savePath, false)
    val list = new ListBuffer[LocatedFileStatus]
    while (fsIterator.hasNext) {
      val file = fsIterator.next()
      if (file.getPath.getName.endsWith(".orc")) {
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
    val deserializer = getOrcDeserializer(orcSchema, colId)
    val iter = new OrcFileIterator(list)
    val iterRow = iter.map(value =>
      unsafeProjection(deserializer.deserialize(value)))
      .map(value => toRowConverter(value))
    new IterableFetchIterator[Row](iterRow.toIterable)
  }

  private def getOrcDeserializer(orcSchema: StructType, colId: Array[Int]): OrcDeserializer = {
    try {
      val cls = Class.forName("org.apache.spark.sql.execution.datasources.orc.OrcDeserializer")
      val constructor = cls.getDeclaredConstructors.apply(0)
      if (constructor.getParameterCount == 3) {
        constructor.newInstance(new StructType, orcSchema, colId).asInstanceOf[OrcDeserializer]
      } else {
        constructor.newInstance(orcSchema, colId).asInstanceOf[OrcDeserializer]
      }
    } catch {
      case e: Throwable =>
        throw new Exception("Failed to create OrcDeserializer", e)
    }
  }
}

class OrcFileIterator(fileList: ListBuffer[LocatedFileStatus]) extends Iterator[OrcStruct] {

  val iters = fileList.map(x => getOrcFileIterator(x))

  var idx = 0

  override def hasNext: Boolean = {
    if (iters(idx).hasNext) {
      true
    } else {
      iters(idx).close()
      idx < iters.size - 1
    }
  }

  override def next(): OrcStruct = {
    if (iters(idx).hasNext) {
      iters(idx).next()
    } else {
      idx = idx + 1
      iters(idx).next()
    }
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
