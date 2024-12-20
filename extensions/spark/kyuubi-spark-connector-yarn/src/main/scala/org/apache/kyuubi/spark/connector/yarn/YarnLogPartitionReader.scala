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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String

import java.io.{BufferedReader, InputStreamReader}
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

class YarnLogPartitionReader(yarnLogPartition: YarnLogPartition)
  extends PartitionReader[InternalRow] {

  private val logIterator = fetchLog().iterator

  override def next(): Boolean = logIterator.hasNext

  override def get(): InternalRow = {
    val yarnLog = logIterator.next()
    new GenericInternalRow(Array[Any](
      UTF8String.fromString(yarnLog.appId),
      UTF8String.fromString(yarnLog.user),
      UTF8String.fromString(yarnLog.host),
      UTF8String.fromString(yarnLog.containerId),
      yarnLog.lineNumber,
      UTF8String.fromString(yarnLog.fileName),
      UTF8String.fromString(yarnLog.message)))
  }

  override def close(): Unit = {
    fs.close()
  }

  /**
   * fet log
   * * hadoop3:
   * * /tmp/logs/xxx/bucket-xxx-tfile/0001/application_1734531705578_0001/localhost_32422
   * * /tmp/logs/xxx/bucket-logs-tfile/0001/application_1734530210878_0001/localhost_24232
   * * hadoop2:
   * * /tmp/logs/xxx/logs/application_1716268141594_240044/node10_35254
   *
   * @param logStatus
   * @param user
   * @param containerId
   * @param applicationId
   */
  private def fetchLog(): Seq[LogEntry] = {
    val logDirInReg = yarnLogPartition.remoteAppLogDir match {
      // in case of /tmp/logs/, /tmp/logs//
      case dir if dir.endsWith("/") =>
        val tmpDir = dir.replaceAll("/+", "/")
        tmpDir.substring(0, tmpDir.length - 1).replace("/", "\\/")
      // in case of /tmp/logs
      case dir => dir.replace("/", "\\/")
    }
    val pathPattern: Regex =
      s".*${logDirInReg}/(.*?)/.*?/(application_.*?)/(.+)_(\\d+)".r
    yarnLogPartition.logPath match {
      case pathPattern(user, applicationId, containerHost, containerSuffix) =>
        val path = new Path(yarnLogPartition.logPath)
        val hadoopConf = new Configuration()
        yarnLogPartition.hadoopConfMap.foreach(kv => hadoopConf.set(kv._1, kv._2))
        val fs = path.getFileSystem(hadoopConf)
        val inputStream = fs.open(path)
        val reader = new BufferedReader(new InputStreamReader(inputStream))
        var line: String = null
        var lineNumber: Int = 1
        val logEntries = new ArrayBuffer[LogEntry]()
        try {
          while ({
            line = reader.readLine()
            line != null
          }) {
            lineNumber += 1
            logEntries += LogEntry(
              applicationId,
              user,
              s"${containerHost}_${containerSuffix}",
              containerHost,
              lineNumber,
              path.getName,
              line)
          }
          logEntries
        } finally {
          IOUtils.closeStream(inputStream)
          reader.close()
        }
      case _ => Seq.empty
    }
  }
}

// Helper class to represent log entries
case class LogEntry(
    appId: String,
    user: String,
    containerId: String,
    host: String,
    lineNumber: Int,
    fileName: String,
    message: String)
