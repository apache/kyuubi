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

import org.apache.avro.{Schema => AvroSchema}
import org.apache.avro.file.{DataFileReader, DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.mapred.FsInput
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.hadoop.io.IOUtils

import org.apache.kyuubi.sql.compact.{CompressionCodecsUtil, MergingFile}

class AvroFileMerger(dataSource: String, codec: Option[String])
  extends AbstractFileMerger(dataSource, codec) {
  override protected def mergeFiles(
      fileSystem: FileSystem,
      smallFiles: List[MergingFile],
      mergedFileInStaging: HadoopPath): Try[HadoopPath] = Try {
    val schema = getAvroSchema(new HadoopPath(location, smallFiles.head.name))

    val recordWriter = new GenericDatumWriter[GenericRecord]
    val outputStream = fileSystem.create(mergedFileInStaging)
    try {
      val fileWriter = new DataFileWriter[GenericRecord](recordWriter)
        .create(schema, outputStream)

      smallFiles.map(f => new HadoopPath(location, f.name)).foreach { file =>
        val fileInput = fileSystem.open(file)
        fileWriter.appendAllFrom(
          new DataFileStream[GenericRecord](fileInput, new GenericDatumReader),
          false)
        IOUtils.closeStream(fileInput)
      }
      IOUtils.closeStream(fileWriter)
    } finally {
      IOUtils.closeStream(outputStream)
    }
    mergedFileInStaging
  }

  private def getAvroSchema(filePath: HadoopPath): AvroSchema = {
    val recordReader = new GenericDatumReader[GenericRecord]
    val avroReader = new DataFileReader(new FsInput(filePath, hadoopConf), recordReader)
    val schema = avroReader.getSchema
    avroReader.close()
    schema
  }

  override protected def getMergedFileNameExtension: String =
    codec.flatMap(CompressionCodecsUtil.getCodecExtension)
      .map(e => s"$e.avro").getOrElse("avro")
}
