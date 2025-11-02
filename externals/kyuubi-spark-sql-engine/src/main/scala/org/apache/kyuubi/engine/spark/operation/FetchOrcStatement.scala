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

import java.util.concurrent.{Executors, ExecutorService}

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

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf.{OPERATION_RESULT_PREFETCH_QUEUE_SIZE, OPERATION_RESULT_PREFETCH_TIMEOUT}
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil.getSessionConf
import org.apache.kyuubi.operation.{FetchIterator, IterableFetchIterator}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion
import org.apache.kyuubi.util.NamedThreadFactory

class FetchOrcStatement(spark: SparkSession) extends Logging {

  var orcIter: OrcFileIterator = _
  var fetchThreadPool: ExecutorService = _

  def getIterator(
      path: String,
      orcSchema: StructType,
      protocolVersion: TProtocolVersion,
      asyncFetchEnabled: Boolean): FetchIterator[Row] = {
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
    if (asyncFetchEnabled) {
      info(f"Creating thread pool for result prefetching")
      fetchThreadPool =
        Executors.newFixedThreadPool(1, new NamedThreadFactory("Result-Prefetch-Pool", false))
      val asyncFetchTimeout: Long = getSessionConf(OPERATION_RESULT_PREFETCH_TIMEOUT, spark)
      val rowsetsQueueSize: Int = getSessionConf(OPERATION_RESULT_PREFETCH_QUEUE_SIZE, spark)
      new IterableAsyncFetchIterator[Row](
        new Iterable[Row] {
          override def iterator: Iterator[Row] = iterRow
        },
        fetchThreadPool,
        orcSchema,
        protocolVersion,
        asyncFetchTimeout,
        rowsetsQueueSize)
    } else {
      new IterableFetchIterator[Row](new Iterable[Row] {
        override def iterator: Iterator[Row] = iterRow
      })
    }

    new IterableFetchIterator[Row](new Iterable[Row] {
      override def iterator: Iterator[Row] = iterRow
    })
  }

  def close(): Unit = {
    orcIter.close()
    if (fetchThreadPool != null) {
      fetchThreadPool.shutdown()
      info(f"Closing thread pool of result prefetching")
    }
  }
}

class OrcFileIterator(fileList: ListBuffer[LocatedFileStatus]) extends Iterator[OrcStruct] {

  private var idx = 0
  private var curIter = getNextIter

  private def getNextIter: Option[RecordReaderIterator[OrcStruct]] = {
    if (idx >= fileList.size) return None
    val resIter = getOrcFileIterator(fileList(idx))
    idx = idx + 1
    Some(resIter)
  }

  override def hasNext: Boolean = {
    if (curIter.isEmpty) return false
    val hasNext = curIter.get.hasNext
    if (!hasNext) {
      curIter.get.close()
      curIter = getNextIter
      // skip empty file
      while (curIter.isDefined) {
        if (curIter.get.hasNext) {
          return true
        } else {
          curIter.get.close()
          curIter = getNextIter
        }
      }
    }
    hasNext
  }

  override def next(): OrcStruct = {
    curIter.get.next()
  }

  def close(): Unit = {
    curIter.foreach(_.close())
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
