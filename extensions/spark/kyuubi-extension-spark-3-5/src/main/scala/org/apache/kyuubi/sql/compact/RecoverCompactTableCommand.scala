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
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.LeafRunnableCommand

import org.apache.kyuubi.sql.compact.merge.AbstractFileMerger

case class RecoverCompactTableCommand(
                                       tableIdentifier: Seq[String],
                                       originalFileLocations: Seq[String]) extends LeafRunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val fileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)

    originalFileLocations.foreach { originalFileLocation =>
      val dataPath = new HadoopPath(originalFileLocation)
      val compactStagingDir = CompactTableUtils.getCompactStagingDir(originalFileLocation)

      if (fileSystem.exists(compactStagingDir)) {
        fileSystem.listStatus(compactStagingDir).foreach { subFolder =>
          log.info(s"delete processing merged files under $subFolder")
          fileSystem.listStatus(
            subFolder.getPath,
            new PathFilter {
              override def accept(path: HadoopPath): Boolean =
                path.getName.startsWith(AbstractFileMerger.mergedFilePrefix) &&
                  path.getName.endsWith(AbstractFileMerger.mergedFileProcessingSuffix)
            }).foreach { f =>
            if (!fileSystem.delete(f.getPath, false)) {
              throw RecoverFileException(s"failed to delete processing merged file ${f.getPath}")
            }
          }

          log.info(s"recover merging files under $subFolder")

          fileSystem.listStatus(
            subFolder.getPath,
            new PathFilter {
              override def accept(path: HadoopPath): Boolean =
                path.getName.startsWith(AbstractFileMerger.mergingFilePrefix)
            }).foreach { smallFile =>
            val fileName = smallFile.getPath.getName
            val recoverFileName =
              fileName.replaceFirst(AbstractFileMerger.mergingFilePrefix + "-\\d+-\\d+-", "")
            if (!fileSystem.rename(smallFile.getPath, new HadoopPath(dataPath, recoverFileName))) {
              throw RecoverFileException(
                s"failed to recover file $fileName to $recoverFileName under $subFolder")
            }
          }

          if (!fileSystem.delete(subFolder.getPath, false)) {
            throw RecoverFileException(s"failed to delete sub folder $subFolder")
          }
          log.info(s"delete sub folder $subFolder")
        }
        if (!fileSystem.delete(compactStagingDir, false)) {
          throw RecoverFileException(s"failed to delete .compact folder $compactStagingDir")
        }
        log.info(s"delete .compact folder $compactStagingDir")

        log.info(s"delete merged files under $originalFileLocation")
        fileSystem.listStatus(
          dataPath,
          new PathFilter {
            override def accept(path: HadoopPath): Boolean = {
              path.getName.startsWith(AbstractFileMerger.mergedFilePrefix) &&
                !path.getName.endsWith(AbstractFileMerger.mergedFileProcessingSuffix)
            }
          }).foreach { mergedFile =>
          if (!fileSystem.delete(mergedFile.getPath, false)) {
            throw RecoverFileException(s"can't delete merged file $mergedFile")
          }
        }
      } else {
        log.info(s"no .compact folder found skip to recover $originalFileLocation")
      }
    }
    log.warn("all files recovered")
    Seq.empty
  }
}
