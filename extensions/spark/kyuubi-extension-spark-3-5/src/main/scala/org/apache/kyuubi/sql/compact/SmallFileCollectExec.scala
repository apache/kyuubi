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

package org.apache.kyuubi.sql.compact

import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath, PathFilter}
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.sources.DataSourceRegister

import org.apache.kyuubi.sql.compact.merge.AbstractFileMerger

/**
 * aggregate small files to groups, sum of file size in each group
 * is equal to target merge size(for example 256Mb) nearly
 * output
 * smallest file location & size in bytes
 * array of file location & size in bytes, need to be appended to the smallest file
 */
case class SmallFileCollectExec(
    baseRelation: HadoopFsRelation,
    output: Seq[Attribute],
    catalogTable: CatalogTable,
    targetSizeInBytes: Option[Long]) extends LeafExecNode {

  private val dataSource = baseRelation.fileFormat.asInstanceOf[DataSourceRegister].shortName()

  // override def nodeName: String = "SmallFileCollectExec"

  override protected def doExecute(): RDD[InternalRow] = {
    val fileSizeInBytesThreshold =
      targetSizeInBytes.getOrElse(JavaUtils.byteStringAsBytes(session.sqlContext.getConf(
        CompactTable.mergeFileSizeKey,
        "256MB")))
    log.info(s"target merged file size in bytes $fileSizeInBytesThreshold")

    val smallFileLocations =
      CompactTableUtils.getCompactDataDir(catalogTable.storage)
    log.info(s"merge file data in ${smallFileLocations.mkString("[", ",", "]")}")
    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)

    smallFileLocations.foreach { dataPath =>
      val compactStaging = CompactTableUtils.getCompactStagingDir(dataPath)
      if (fileSystem.exists(compactStaging)) {
        throw MergeFileException(
          s"compact staging $compactStaging already exists, " +
            s"you should recover it before compacting")
      }
    }

    val shuffleNum = session.sqlContext.getConf("spark.sql.shuffle.partitions").toInt
    log.info(s"target shuffle num $shuffleNum")
    val smallFileAndLocs = smallFileLocations.flatMap { loc =>
      val smallFiles = getSmallFiles(fileSystem, new HadoopPath(loc), fileSizeInBytesThreshold)
      if (smallFiles.nonEmpty) {
        val codec = getCodecFromFile(smallFiles.head.name)
        val neededMergeFileGroups = aggregateSmallFile(smallFiles, fileSizeInBytesThreshold)
          .map { group =>
            MergingFilePartition(-1, loc, dataSource, codec, group)
          }

        val groupNum = neededMergeFileGroups.length
        val shortNeededMergeFileGroups =
          if (groupNum > 1 &&
            neededMergeFileGroups.apply(groupNum - 1).smallFiles.map(
              _.length).sum < fileSizeInBytesThreshold) {
            val last2 = neededMergeFileGroups.apply(groupNum - 2)
            val last1 = neededMergeFileGroups.apply(groupNum - 1)
            val merged = last2.copy(smallFiles = last2.smallFiles ++ last1.smallFiles)
            neededMergeFileGroups.dropRight(2) :+ merged
          } else {
            neededMergeFileGroups
          }

        val originalGroupSize = shortNeededMergeFileGroups.length
        val groupEleNum = Math.max(Math.floorDiv(originalGroupSize, shuffleNum), 1)

        val regroupSmallFileAndLocs =
          shortNeededMergeFileGroups.sliding(groupEleNum, groupEleNum).map { groups =>
            val newGroup = groups.zipWithIndex.map { case (group, subIndex) =>
              group.smallFiles.map(_.copy(subGroupId = subIndex))
            }
            MergingFilePartition(-1, loc, dataSource, codec, newGroup.flatten)
          }.toList
        regroupSmallFileAndLocs
      } else {
        Iterator.empty
      }
    }.toArray.zipWithIndex.map {
      case (part, globalIndex) => part.copy(index = globalIndex, groupId = globalIndex)
    }
    new FileMergingRDD(
      session,
      CompactTable.smallFileCollectOutput,
      smallFileAndLocs)
  }

  private def getCodecFromFile(filePath: String): Option[String] =
    CompactTableUtils.getCodecFromFilePath(
      new HadoopPath(filePath),
      sparkContext.hadoopConfiguration)

  private def getSmallFiles(
      fs: FileSystem,
      location: HadoopPath,
      fileSizeInBytes: Long): Array[MergingFile] = {
    fs.listStatus(
      location,
      new PathFilter {
        override def accept(path: HadoopPath): Boolean = {
          val pathName = path.getName
          !(pathName.startsWith(".") || pathName.startsWith("_") || pathName.startsWith(
            AbstractFileMerger.mergingFilePrefix) || pathName.startsWith(
            AbstractFileMerger.mergedFilePrefix))
        }
      }).filter(_.getLen < fileSizeInBytes)
      .map(file => MergingFile(0, file.getPath.getName, file.getLen))
      .sortBy(_.length)
  }

  private def aggregateSmallFile(
      sortedSmallFiles: Array[MergingFile],
      targetFileSizeInBytes: Long): List[Seq[MergingFile]] = {
    var groupedFiles: List[Seq[MergingFile]] = List.empty
    var start = 0
    var end = sortedSmallFiles.length
    var sizeSum = 0L
    while (start < sortedSmallFiles.length) {
      sizeSum = 0L
      start until sortedSmallFiles.length takeWhile { i =>
        sizeSum += sortedSmallFiles(i).length
        end = i
        sizeSum < targetFileSizeInBytes
      }
      groupedFiles = groupedFiles :+ sortedSmallFiles.slice(start, end + 1).toSeq
      start = end + 1
    }
    groupedFiles
  }
}
