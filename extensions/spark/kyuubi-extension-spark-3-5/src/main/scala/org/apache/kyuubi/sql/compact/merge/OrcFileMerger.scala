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

import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.orc.OrcFile

import org.apache.kyuubi.sql.compact.{CompactTableUtils, CompressionCodecsUtil, MergingFile}

class OrcFileMerger(dataSource: String, codec: Option[String])
  extends AbstractFileMerger(dataSource, codec) {
  override protected def mergeFiles(
      fileSystem: FileSystem,
      smallFiles: List[MergingFile],
      mergedFileInStaging: HadoopPath): Try[HadoopPath] = Try {
    val smallFilePaths = smallFiles.map(r => new HadoopPath(location, r.name))
    val writerOptions = OrcFile.writerOptions(hadoopConf)
    val mergedFiles =
      OrcFile.mergeFiles(
        mergedFileInStaging,
        writerOptions,
        CompactTableUtils.toJavaList(smallFilePaths))

    if (smallFilePaths.length != mergedFiles.size) {
      val unMergedFiles = smallFilePaths.filterNot(mergedFiles.contains)
      throw new IllegalStateException(
        s"Failed to merge files: ${unMergedFiles.mkString}")
    }
    mergedFileInStaging
  }

  override protected def getMergedFileNameExtension: String =
    codec.flatMap(CompressionCodecsUtil.getCodecExtension)
      .map(e => s"$e.orc").getOrElse("orc")
}
