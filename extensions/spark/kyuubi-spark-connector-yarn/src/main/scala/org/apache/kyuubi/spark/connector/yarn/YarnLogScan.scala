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

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class YarnLogScan(
    options: CaseInsensitiveStringMap,
    schema: StructType,
    filters: Array[Filter])
  extends BasicScan {
  override def readSchema(): StructType = schema

  private val remoteAppLogDirKeyInYarnSite = "yarn.nodemanager.remote-app-log-dir"
  private val remoteAppLogDirKey = "spark.sql.catalog.yarn.log.dir"

  private val remoteAppLogDir = {
    val dir = SparkSession.active.sparkContext
      .getConf.getOption(remoteAppLogDirKey) match {
      case Some(dir) => Some(dir)
      case _ => hadoopConfMap.get(remoteAppLogDirKeyInYarnSite)
    }
    if (dir.isEmpty) {
      throw new IllegalArgumentException(
        s"remoteAppLogDir should be set with ${remoteAppLogDirKey} or set with " +
          s"${remoteAppLogDirKeyInYarnSite} in yarn-site.xml")
    }
    dir.get
  }

  // given a path in hdfs, then get all files under it, supports *
  private def listFiles(pathStr: String): mutable.Seq[FileStatus] = {
    val hadoopConf = new Configuration()
    hadoopConfMap.foreach(kv => hadoopConf.set(kv._1, kv._2))
    val fs = FileSystem.get(hadoopConf)
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
    fs.close()
    logFiles
  }

  /**
   * pushdown equalTo
   * hadoop3:
   * /tmp/logs/xxx/bucket-xxx-tfile/0001/application_1734531705578_0001/localhost_32422
   * /tmp/logs/xxx/bucket-logs-tfile/0001/application_1734530210878_0001/localhost_24232
   *
   * @return
   */
  private def tryPushDownPredicates(): mutable.Seq[FileStatus] = {
    filters match {
      case pushed if pushed.isEmpty => listFiles(remoteAppLogDir)
      case pushed => pushed.collectFirst {
          case EqualTo("app_id", appId: String) =>
            listFiles(s"${remoteAppLogDir}/*/*/*/${appId}")
          case EqualTo("container_id", containerId: String) =>
            listFiles(s"${remoteAppLogDir}/*/*/*/*/${containerId}")
          case EqualTo("user", user: String) => listFiles(s"${remoteAppLogDir}/${user}")
          case _ => listFiles(remoteAppLogDir)
        }.get
    }
  }

  override def planInputPartitions(): Array[InputPartition] = {
    // get file nums and construct nums inputPartition
    tryPushDownPredicates().map(fileStatus => {
      YarnLogPartition(hadoopConfMap, fileStatus.getPath.toString, remoteAppLogDir)
    }).toArray
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new YarnLogReaderFactory

}
