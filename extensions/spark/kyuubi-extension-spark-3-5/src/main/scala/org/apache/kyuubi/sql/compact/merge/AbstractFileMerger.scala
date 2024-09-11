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

package org.apache.kyuubi.sql.compact.merge

import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.spark.internal.Logging
import org.apache.spark.util.SerializableConfiguration

import org.apache.kyuubi.sql.compact.{CompactTableUtils, MergeFileException, MergingFile}
import org.apache.kyuubi.sql.compact.merge.AbstractFileMerger.{getMergedFilePrefix, getMergingFilePrefix}

object AbstractFileMerger {
  val mergedFilePrefix = "merged"
  val mergedFileProcessingSuffix = ".processing"
  val mergingFilePrefix = "merging"

  def getMergingFilePrefix(groupId: Int, subGroupId: Int): String =
    s"$mergingFilePrefix-$groupId-$subGroupId"

  def getMergedFilePrefix(groupId: Int, subGroupId: Int): String =
    s"$mergedFilePrefix-$groupId-$subGroupId"
}

abstract class AbstractFileMerger(dataSource: String, codec: Option[String]) extends Logging
  with Serializable {

  protected var partitionIndex: Int = _
  protected var jobId: String = _
  protected var groupId: Int = _
  protected var location: String = _
  protected var serializableConfiguration: SerializableConfiguration = _
  protected var isMergeMetadata: Boolean = _

  def initialize(
                  partitionIndex: Int,
                  jobId: String,
                  groupId: Int,
                  location: String,
                  serializableConfiguration: SerializableConfiguration,
                  isMergeMetadata: Boolean): Unit = {
    this.partitionIndex = partitionIndex
    this.jobId = jobId
    this.groupId = groupId
    this.location = location
    this.serializableConfiguration = serializableConfiguration
    this.isMergeMetadata = isMergeMetadata
  }

  def merge(smallFiles: List[MergingFile]): Try[Array[MergingFile]] = Try {
    val fileSystem: FileSystem = FileSystem.get(hadoopConf)

    smallFiles.foreach { merging =>
      log.info(
        s"merging files jobId $jobId, partition id $partitionIndex,group id $groupId,$merging")
    }

    smallFiles.groupBy(_.subGroupId).map { case (subGroupId, fileGroup) =>
      mergeGroup(fileSystem, subGroupId, fileGroup).map(m =>
        MergingFile(subGroupId, m._1.getName, m._2)).get
    }.toArray

  }

  private def mergeGroup(
                          fileSystem: FileSystem,
                          subGroupId: Int,
                          smallFiles: List[MergingFile]): Try[(HadoopPath, Long)] = Try {
    val stagingDir = CompactTableUtils.getStagingDir(location, jobId)
    val locationPath = new HadoopPath(location)
    val mergedFileName = s"${getMergedFilePrefix(groupId, subGroupId)}.$getMergedFileNameExtension"
    val mergedFileInStaging =
      new HadoopPath(stagingDir, mergedFileName + AbstractFileMerger.mergedFileProcessingSuffix)
    val targetMergedFile = new HadoopPath(locationPath, mergedFileName)
    log.info(s"prepare to merge $dataSource files to $mergedFileInStaging")
    mergeFiles(fileSystem, smallFiles, mergedFileInStaging).get
    val mergingFilePrefix = getMergingFilePrefix(groupId, subGroupId)
    log.info(s"prepare to add prefix ${mergingFilePrefix} to small files")
    val stagingSmallFiles = smallFiles.map(_.name).map { fileName =>
      val smallFile = new HadoopPath(location, fileName)
      val newFileName = s"$mergingFilePrefix-$fileName"
      val smallFileNewPath = new HadoopPath(location, newFileName)
      if (!fileSystem.rename(smallFile, smallFileNewPath)) {
        throw MergeFileException(s"failed to rename $smallFile to $smallFileNewPath")
      }
      smallFileNewPath
    }
    log.info(s"move file $mergedFileInStaging to $targetMergedFile")
    if (fileSystem.exists(targetMergedFile)) {
      throw MergeFileException(s"file already exists $targetMergedFile")
    }
    if (!fileSystem.rename(mergedFileInStaging, targetMergedFile)) {
      throw MergeFileException(s"failed to rename $mergedFileInStaging to $targetMergedFile")
    }

    log.info(s"move small files to $stagingDir")
    stagingSmallFiles.foreach { smallFilePath =>
      val stagingFile = new HadoopPath(stagingDir, smallFilePath.getName)
      if (!fileSystem.rename(smallFilePath, stagingFile)) {
        throw MergeFileException(s"failed to rename $smallFilePath to $stagingFile")
      }
    }
    (targetMergedFile, fileSystem.getFileStatus(targetMergedFile).getLen)
  }

  protected def hadoopConf: Configuration = serializableConfiguration.value

  protected def mergeFiles(
                            fileSystem: FileSystem,
                            smallFiles: List[MergingFile],
                            mergedFileInStaging: HadoopPath): Try[HadoopPath]

  protected def getMergedFileNameExtension: String

}
