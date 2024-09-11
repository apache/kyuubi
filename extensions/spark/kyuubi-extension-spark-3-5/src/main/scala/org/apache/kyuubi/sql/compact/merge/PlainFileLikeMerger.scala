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
import org.apache.hadoop.io.IOUtils

import org.apache.kyuubi.sql.compact.{CompressionCodecsWrapper, MergingFile}

class PlainFileLikeMerger(dataSource: String, codec: Option[String])
  extends AbstractFileMerger(dataSource, codec) {
  override protected def mergeFiles(
                                     fileSystem: FileSystem,
                                     smallFiles: List[MergingFile],
                                     mergedFileInStaging: HadoopPath): Try[HadoopPath] = Try {
    val smallFilePaths = smallFiles.map(r => new HadoopPath(location, r.name))
    val fos = fileSystem.create(mergedFileInStaging, false)
    try {
      // refer to org.apache.hadoop.fs.shell.CopyCommands.AppendToFile
      smallFilePaths.foreach { f =>
        log.info(s"copying file $f")
        val is = fileSystem.open(f)
        IOUtils.copyBytes(is, fos, hadoopConf, false)
        IOUtils.closeStream(is)
      }
    } finally {
      IOUtils.closeStream(fos)
    }
    mergedFileInStaging
  }

  override protected def getMergedFileNameExtension: String =
    codec.flatMap(CompressionCodecsWrapper.getCodecExtension)
      .map(e => s"$dataSource.$e").getOrElse(dataSource)
}
