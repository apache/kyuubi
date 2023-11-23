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

package org.apache.spark.sql.execution.compactor

import java.util.{ArrayList, Arrays, Collection, Date, HashMap => JHashMap, HashSet => JHashSet, LinkedHashSet, List => JList, Locale, Set => JSet}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter, ParquetWriter}
import org.apache.parquet.hadoop.metadata.{BlockMetaData, FileMetaData, GlobalMetaData, ParquetMetadata}
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}
import org.apache.parquet.schema.MessageType

class ParquetCompactor extends Compactor {

  override protected def compactInternal(
      conf: Configuration,
      src: Array[Path],
      dest: Path): Unit = {
    val mergedMeta = mergeMetadataFiles(src, conf).getFileMetaData
    val writer = new ParquetFileWriter(
      HadoopOutputFile.fromPath(dest, conf),
      mergedMeta.getSchema,
      ParquetFileWriter.Mode.CREATE,
      ParquetWriter.DEFAULT_BLOCK_SIZE,
      ParquetWriter.MAX_PADDING_SIZE_DEFAULT)
    writer.start()
    src.foreach { input =>
      writer.appendFile(HadoopInputFile.fromPath(input, conf))
    }
    writer.end(mergedMeta.getKeyValueMetaData)
  }

  private def mergeMetadataFiles(files: Array[Path], conf: Configuration): ParquetMetadata = {
    if (files.isEmpty) {
      throw new IllegalArgumentException("Cannot merge an empty list of metadata")
    }
    var globalMetaData: GlobalMetaData = null
    val blocks = new ArrayList[BlockMetaData]

    for (p <- files) {
      val file = HadoopInputFile.fromPath(p, conf)
      val options =
        HadoopReadOptions.builder(conf).withMetadataFilter(ParquetMetadataConverter.NO_FILTER).build
      val reader = new ParquetFileReader(file, options)
      val pmd = reader.getFooter
      val fmd = reader.getFileMetaData
      val currentRange = s"${blocks.size()}_${blocks.size() + pmd.getBlocks.size()}"
      globalMetaData = mergeInto(currentRange, fmd, globalMetaData, true)
      blocks.addAll(pmd.getBlocks)
    }
    //  collapse GlobalMetaData into a single FileMetaData,
    //  which will throw if they are not compatible.
    new ParquetMetadata(globalMetaData.merge, blocks)
  }

  private def mergeInto(
      currentRange: String,
      toMerge: FileMetaData,
      mergedMetadata: GlobalMetaData,
      strict: Boolean): GlobalMetaData = {
    var schema: MessageType = null
    val newKeyValues = new JHashMap[String, JSet[String]]()
    val createdBy = new JHashSet[String]()
    if (mergedMetadata != null) {
      schema = mergedMetadata.getSchema()
      newKeyValues.putAll(mergedMetadata.getKeyValueMetaData)
      createdBy.addAll(mergedMetadata.getCreatedBy)
    }
    if ((schema == null && toMerge.getSchema != null) ||
      (schema != null && !schema.equals(toMerge.getSchema))) {
      schema = mergeInto(toMerge.getSchema, schema, strict)
    }
    toMerge.getKeyValueMetaData.entrySet.asScala.foreach { entry =>
      if (entry.getKey.equals("SOURCE_IP")) {
        val valueSet = new LinkedHashSet[String](1)
        Option(newKeyValues.get(entry.getKey)).map(_.toArray.head) match {
          case Some(oldIps) =>
            valueSet.add(oldIps + s", $currentRange:${entry.getValue}")
          case None =>
            valueSet.add(s"$currentRange:${entry.getValue}")
        }
        newKeyValues.put(entry.getKey, valueSet)
      } else {
        var values = newKeyValues.get(entry.getKey)
        if (values == null) {
          values = new LinkedHashSet[String]()
          newKeyValues.put(entry.getKey, values)
        }
        values.add(entry.getValue)
      }
    }
    createdBy.add(toMerge.getCreatedBy)
    new GlobalMetaData(schema, newKeyValues, createdBy)
  }

  private def mergeInto(
      toMerge: MessageType,
      mergedSchema: MessageType,
      strict: Boolean): MessageType = {
    if (mergedSchema == null) {
      toMerge
    } else {
      mergedSchema.union(toMerge, strict)
    }
  }
}
