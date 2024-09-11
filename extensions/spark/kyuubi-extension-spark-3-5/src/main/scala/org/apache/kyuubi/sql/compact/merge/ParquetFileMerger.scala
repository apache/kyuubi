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

import java.util

import scala.util.Try

import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter}
import org.apache.parquet.hadoop.metadata.{BlockMetaData, FileMetaData, GlobalMetaData, ParquetMetadata}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.SparkInternalExplorer

import org.apache.kyuubi.sql.ParquetFileWriterWrapper
import org.apache.kyuubi.sql.compact.{CompressionCodecsWrapper, MergingFile}

class ParquetFileMerger(dataSource: String, codec: Option[String])
  extends AbstractFileMerger(dataSource, codec) {

  override protected def mergeFiles(
      fileSystem: FileSystem,
      smallFiles: List[MergingFile],
      mergedFileInStaging: HadoopPath): Try[HadoopPath] = Try {
    val smallFilePaths = smallFiles.map(r => new HadoopPath(location, r.name))

    val metadataFiles = if (isMergeMetadata) smallFilePaths else smallFilePaths.take(1)
    log.info(s"merge metadata of files ${metadataFiles.length}")
    val mergedMetadata = mergeMetadata(metadataFiles)
    // ParquetOutputFormat.MAX_PADDING_BYTES,ParquetOutputFormat.BLOCK_SIZE
    val writer = new ParquetFileWriter(
      hadoopConf,
      mergedMetadata.getSchema,
      mergedFileInStaging,
      ParquetFileWriter.Mode.CREATE)
    log.info(
      s"begin to merge parquet files to $mergedFileInStaging from ${smallFilePaths.length} files")

    writer.start()
    smallFilePaths.foreach { smallFile =>
      writer.appendFile(HadoopInputFile.fromPath(smallFile, hadoopConf))
    }
    writer.end(mergedMetadata.getKeyValueMetaData)

    log.info(s"finish to merge parquet files to $mergedFileInStaging")

    mergedFileInStaging
  }

  private def mergeMetadata(files: List[HadoopPath]): FileMetaData = {
    var globalMetaData: GlobalMetaData = null
    val blocks: util.List[BlockMetaData] = new util.ArrayList[BlockMetaData]()
    SparkInternalExplorer.parmap(files, "readingParquetFooters", 8) {
      currentFile =>
        ParquetFileReader.readFooter(
          hadoopConf,
          currentFile,
          ParquetMetadataConverter.NO_FILTER)
    }.foreach { pmd =>
      val fmd = pmd.getFileMetaData
      globalMetaData = ParquetFileWriterWrapper.mergeInto(fmd, globalMetaData, strict = true)
      blocks.addAll(pmd.getBlocks)
    }
    new ParquetMetadata(globalMetaData.merge(), blocks).getFileMetaData
  }

  override protected def getMergedFileNameExtension: String =
    codec.flatMap(CompressionCodecsWrapper.getCodecExtension)
      .map(e => s"$e.parquet").getOrElse("parquet")
}
