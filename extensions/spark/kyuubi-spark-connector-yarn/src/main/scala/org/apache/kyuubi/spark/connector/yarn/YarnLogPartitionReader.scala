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

import java.io.{BufferedReader, InputStreamReader}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources.EqualTo

class YarnLogPartitionReader(yarnLogPartition: YarnLogPartition)
  extends PartitionReader[InternalRow] {

  private val remoteAppLogDirKey = "yarn.nodemanager.remote-app-log-dir"

  private val fs = {
    val hadoopConf = new Configuration()
    yarnLogPartition.hadoopConfMap.foreach(kv => hadoopConf.set(kv._1, kv._2))
    FileSystem.get(hadoopConf)
  }

  private val logsIterator = fetchLogs().iterator

  override def next(): Boolean = logsIterator.hasNext

  override def get(): InternalRow = {
    val log = logsIterator.next()
    // new GenericInternalRow(Array[Any](log.appId, log.logLevel, log.message))
    null
  }

  // given a path in hdfs, then get all files under it, supports *
  def listFiles(pathStr: String): mutable.Seq[FileStatus] = {
    val path = new Path(pathStr)
    val logFiles = mutable.ArrayBuffer[FileStatus]()
    if (fs.exists(path)) {
      val fileStatuses: Array[FileStatus] = fs.globStatus(path)
      if (fileStatuses != null && fileStatuses.nonEmpty) {
        fileStatuses.foreach {
          case status if status.isFile => logFiles += status
          case status if status.isDirectory =>
            val fileIterator = fs.listFiles(status.getPath, true)
            while (fileIterator.hasNext) {
              val fileStatus = fileIterator.next()
              if (fileStatus.isFile) logFiles += fileStatus
            }
        }
      }
    }
    logFiles
  }

  // /tmp/logs/xxx/bucket-xxx-tfile/0001/application_1734531705578_0001/localhost_32422
  // /tmp/logs/xxx/bucket-logs-tfile/0001/application_1734530210878_0001/localhost_24232
  // /tmp/logs/xxx/logs/application_1716268141594_240044/node10_35254

  override def close(): Unit = {
    fs.close()
  }

  private def fetchLogs(): Seq[LogEntry] = {
    // Simulate fetching logs for the given appId (replace with Yarn API calls)
    val remoteAppLogDir = yarnLogPartition.logPath match {
      case Some(dir) => Some(dir)
      case _ => yarnLogPartition.hadoopConfMap.get(remoteAppLogDirKey)
    }
    // TODO throw exception here
    val logFileStatuses = remoteAppLogDir match {
      case Some(dir) =>
        yarnLogPartition.filters match {
          case filters if filters.isEmpty => listFiles(remoteAppLogDir.get)
          case filters => filters.collectFirst {
              case EqualTo("app_id", appId: String) =>
                listFiles(s"${remoteAppLogDir}/*/*/*/*/${appId}")
              // TODO hadoop2 listFiles(s"${remoteAppLogDir}/*/*/*/${appId}")
              case EqualTo("user", user: String) => listFiles(s"${remoteAppLogDir}/${user}")
              case _ => listFiles(remoteAppLogDir.get)
            }.get
        }
      case _ => mutable.Seq.empty
    }
    val logEntries = new ArrayBuffer[LogEntry]()
    logFileStatuses.foreach { logFileStatus =>
      {
        val split = logFileStatus.getPath.toString.split("/")
        val containerId = split(split.length - 1)
        val applicationId = split(split.length - 2)
        // TODO use regexp
        val user = remoteAppLogDir.get match {
          case dir if dir.startsWith("hdfs") =>
            logFileStatus.getPath.toString.split(s"${dir}")(0).split("/")(0)
          case dir => logFileStatus.getPath.toString.split(s"${dir}")(1).split("/")(0)
        }
        logFileStatus.getPath.toString.split(s"${remoteAppLogDir}")(1).split("/")(0)
        // todo read logs multi-threads
        logEntries ++= fetchLog(
          logFileStatus,
          user,
          containerId,
          applicationId)
      }
    }
    Seq()
  }

  /**
   * fet log
   * @param logStatus
   * @param user
   * @param containerId
   * @param applicationId
   */
  private def fetchLog(
      logStatus: FileStatus,
      user: String,
      containerId: String,
      applicationId: String): Seq[LogEntry] = {
    val path = logStatus.getPath
    val inputStream = fs.open(path)
    val reader = new BufferedReader(new InputStreamReader(inputStream))
    var line: String = null
    var lineNumber: Int = 1
    val logEntries = new ArrayBuffer[LogEntry]()
    try {
      while ({ line = reader.readLine(); line != null }) {
        // println(s"Line $lineNumber: $line")
        lineNumber += 1
        logEntries += LogEntry(
          applicationId,
          user,
          containerId,
          lineNumber,
          line)
      }
      logEntries
    } finally {
      // 关闭流
      IOUtils.closeStream(inputStream)
      reader.close()
    }
  }
}

// Helper class to represent log entries
case class LogEntry(
    appId: String,
    user: String,
    containerId: String,
    rowNumber: Int,
    message: String)
