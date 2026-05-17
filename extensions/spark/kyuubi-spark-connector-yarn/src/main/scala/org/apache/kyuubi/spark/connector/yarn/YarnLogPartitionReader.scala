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

package org.apache.kyuubi.spark.connector.yarn

import java.io.{BufferedReader, DataInputStream, EOFException, InputStreamReader, IOException}
import java.lang.{Long => JLong}
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

case class YarnLogPartition(
    serializableHadoopConf: SerializableConfiguration,
    filePath: String,
    length: Long,
    mtime: Long) extends InputPartition

class YarnLogPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new YarnLogPartitionReader(partition.asInstanceOf[YarnLogPartition])
  }
}

/**
 * Streaming reader for a single YARN aggregated log file.
 *
 * Path layout (Hadoop 3, file-controller = TFile):
 *   {remoteAppLogDir}/{user}/bucket-{suffix}-tfile/{bucket}/{appId}/{host}_{port}
 * e.g. /tmp/logs/root/bucket-logs-tfile/0001/application_1734530210878_0001/localhost_24232
 *
 * The aggregated log file is a TFile whose entries are keyed by container id. Each value
 * stream contains a sequence of `(UTF logFileName, UTF logFileLength, raw bytes)` tuples,
 * terminated by [[EOFException]] on the next `readUTF()`.
 *
 * This reader keeps a small amount of state and emits one row per line while it walks the
 * file, so memory usage is independent of the log size.
 */
class YarnLogPartitionReader(split: YarnLogPartition)
  extends PartitionReader[InternalRow] with Logging {

  private val hadoopConf = split.serializableHadoopConf.value
  private val path = new Path(split.filePath)

  // Constant per-partition columns derived from the file path.
  private val (appId, user, host) = parsePath(path)

  private lazy val logReader: AggregatedLogFormat.LogReader = {
    logInfo(s"Task (TID ${TaskContext.get().taskAttemptId()}) input split: " +
      s"${split.filePath}:0-${split.length}")
    new AggregatedLogFormat.LogReader(hadoopConf, path)
  }

  // Current container being read; null when no more containers.
  private var key: AggregatedLogFormat.LogKey = new AggregatedLogFormat.LogKey
  private var valueStream: DataInputStream = logReader.next(key)
  private var containerId: String = if (valueStream != null) key.toString else null

  // Current file (e.g. stdout / stderr) within the current container.
  private var currentLogType: String = _
  private var currentLogTypeLength: Long = 0L
  private var currentBoundedInput: BoundedInputStream = _
  private var currentReader: BufferedReader = _

  // Current line being held for `get()`.
  private var currentLineNum: Int = 0
  private var currentMessage: String = _

  private var closed: Boolean = false

  override def next(): Boolean = {
    if (closed) return false
    while (true) {
      // 1) Try to pull the next line from the current file.
      if (currentReader != null) {
        // TODO add a max line length threshold to avoid OOM on malformed logs
        //      or non-text files.
        val line = currentReader.readLine()
        if (line != null) {
          currentLineNum += 1
          currentMessage = line
          return true
        }

        currentBoundedInput.skip(currentLogTypeLength)
        currentReader = null
        currentBoundedInput = null
        currentLogType = null
        currentLogTypeLength = 0L
      }

      // 2) No current file; try to open the next file in the current container.
      if (valueStream != null) {
        if (!openNextLogType()) {
          // EOF on this container; advance to the next container and loop again.
          advanceContainer()
        }
      } else {
        // 3) No more containers, no more files.
        return false
      }
    }
    false // unreachable
  }

  override def get(): InternalRow = {
    new GenericInternalRow(Array[Any](
      UTF8String.fromString(appId),
      UTF8String.fromString(user),
      UTF8String.fromString(host),
      UTF8String.fromString(containerId),
      UTF8String.fromString(currentLogType),
      currentLineNum,
      UTF8String.fromString(currentMessage), // NULLABLE
      TimeUnit.MILLISECONDS.toMicros(split.mtime)))
  }

  override def close(): Unit = if (!closed) {
    closed = true
    closeQuietly(valueStream)
    // YARN-10855 (3.4.0) AggregatedLogFormat.LogReader starts to implement AutoCloseable
    closeQuietly(logReader)
  }

  private def closeQuietly(maybeCloseable: Any): Unit = maybeCloseable match {
    case null =>
    case closeable: AutoCloseable =>
      try closeable.close()
      catch {
        case ioe: IOException => logWarning("IOException should not have been thrown.", ioe)
      }
    case _ =>
  }

  /**
   * Read the next `(logType, logTypeLength, bytes)` tuple from the current container's
   * value stream and prepare a [[BufferedReader]] over its bytes.
   *
   * @return true on success; false if the container's stream is exhausted (EOF).
   */
  private def openNextLogType(): Boolean = {
    try {
      currentLogType = valueStream.readUTF()
      currentLogTypeLength = JLong.parseLong(valueStream.readUTF())
      logInfo(s"Reading container $containerId log type $currentLogType " +
        s"(length $currentLogTypeLength)")
      currentLineNum = 0
      // BoundedInputStream caps reads at logFileLength so the next readUTF() lines up with
      // the next file's metadata. Do NOT close it - closing would propagate to valueStream
      // (BoundedInputStream's propagateClose defaults to true).
      currentBoundedInput = new BoundedInputStream(valueStream, currentLogTypeLength)
      currentReader = new BufferedReader(
        new InputStreamReader(currentBoundedInput, StandardCharsets.UTF_8))
      true
    } catch {
      case _: EOFException => false
    }
  }

  private def advanceContainer(): Unit = {
    key = new AggregatedLogFormat.LogKey
    valueStream = logReader.next(key)
    containerId = if (valueStream != null) key.toString else null
  }

  /**
   * Derive (appId, user, host) from the file path layout
   *   .../{user}/bucket-{suffix}-tfile/{bucket}/{appId}/{host}_{port}<_{timestamp}>
   */
  private def parsePath(path: Path): (String, String, String) = {
    val filename = path.getName // e.g. "hadoop-node-123_24232"
    // according to RFC 1123, _ is not allowed in hostname
    val host = filename.indexOf('_') match {
      case -1 => filename
      case idx => filename.substring(0, idx)
    }
    val appId = Option(path.getParent).map(_.getName).getOrElse("unknown")
    // walk up: appId -> bucket -> bucket-{suffix}-tfile -> user
    var p = path.getParent
    var i = 0
    while (p != null && i < 3) {
      p = p.getParent
      i += 1
    }
    val user = Option(p).map(_.getName).getOrElse("unknown")
    (appId, user, host)
  }
}
