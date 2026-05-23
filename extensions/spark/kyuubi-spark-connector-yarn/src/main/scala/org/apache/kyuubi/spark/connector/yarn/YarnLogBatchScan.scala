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
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

case class YarnLogBatchScan(
    options: CaseInsensitiveStringMap,
    schema: StructType,
    pushedFilters: Array[Filter])
  extends Scan with Batch with Logging {

  override def readSchema(): StructType = schema

  protected val hadoopConf: Configuration =
    SparkSession.active.sessionState.newHadoopConf()

  protected val ignoreMissingFiles: Boolean =
    SparkSession.active.sessionState.conf.ignoreMissingFiles

  private val _formats = hadoopConf.get(YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS, "TFile")
  if (!_formats.equalsIgnoreCase("TFile")) {
    throw new UnsupportedOperationException(
      "Currently, only TFile format is supported. While " +
        s"${YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS} was set to ${_formats}.")
  }

  private val remoteAppLogDir = hadoopConf.get(
    YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
    YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR)

  private val remoteAppLogDirSuffix = hadoopConf.get(
    YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX,
    YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX)

  // given a path in hdfs, then get all files under it, supports *
  private def listFiles(pathStr: String): Array[FileStatus] = {
    val path = new Path(pathStr)
    val fs = path.getFileSystem(hadoopConf)

    val start = System.currentTimeMillis()
    val files = fs.globStatus(path)
    val taken = System.currentTimeMillis() - start
    logInfo(s"Listing $pathStr takes $taken ms, found ${files.length} files")
    files
  }

  private def listFilesWithFilters(): Array[FileStatus] = {
    val baseDir = remoteAppLogDir
    val bucketDir = s"bucket-$remoteAppLogDirSuffix-tfile"
    var path = s"$baseDir/{{USER}}/$bucketDir/{{BUCKET}}/{{APP_ID}}/{{HOST}}_*"
    pushedFilters.foreach {
      case EqualTo("app_id", appId: String) =>
        path = path.replace("{{APP_ID}}", appId)
      case EqualTo("user", user: String) =>
        path = path.replace("{{USER}}", user)
      case EqualTo("host", host: String) =>
        path = path.replace("{{HOST}}", host)
      case f =>
        logWarning(s"Unsupported filter: $f")
    }
    val globPath = path
      .replace("{{BUCKET}}", "*") // TODO parallize bucket listing
      .replace("{{APP_ID}}", "*")
      .replace("{{USER}}", "*")
      .replace("{{HOST}}", "*")

    listFiles(globPath)
  }

  // TODO the API docs say "this method will be called only once during a data source scan",
  //      but in practice, it may be called multiple times, this is likely a Spark side issue.
  override def planInputPartitions(): Array[InputPartition] = {
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    // TODO support pushdown mtime
    // TODO support pushdown log types
    listFilesWithFilters().map { fileStatus =>
      YarnLogPartition(
        serializableHadoopConf,
        ignoreMissingFiles,
        fileStatus.getPath.toString,
        fileStatus.getLen,
        fileStatus.getModificationTime)
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = new YarnLogPartitionReaderFactory

  override def toBatch: Batch = this

  // TODO display the listing path, but need to figure out whether this method will be called
  //      before or after filter pushdown.
  override def description(): String = {
    s"YARN Aggregated Log Scan, pushed filters: ${pushedFilters.mkString(", ")}"
  }
}
