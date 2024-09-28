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

import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.OrcFile

class OrcCompactor extends Compactor {
  override protected def compactInternal(
      conf: Configuration,
      src: Array[Path],
      dest: Path): Unit = {
    val writeOptions = OrcFile.writerOptions(conf)
    val compactSrcPaths = optimizeCompactFrom(src, conf)
    val compactSuccessSrcPaths =
      OrcFile.mergeFiles(dest, writeOptions, compactSrcPaths.toSeq.asJava)
    if (compactSuccessSrcPaths.size() != src.length) {
      val failedCompactFromPaths = compactSrcPaths.filterNot(compactSuccessSrcPaths.contains)
      val allFilesEmpty = isAllFilesEmpty(conf, failedCompactFromPaths)
      if (!allFilesEmpty) {
        throw new IllegalStateException(
          "Merge ORC failed due to files cannot be merged:" + failedCompactFromPaths)
      }
    }
  }

  private def optimizeCompactFrom(
      compactFromPaths: Array[Path],
      conf: Configuration): Array[Path] = {
    val readOptions = OrcFile.readerOptions(conf)
    var index = -1
    breakable {
      for (compactFromPath <- compactFromPaths) {
        index += 1
        val reader = OrcFile.createReader(compactFromPath, readOptions)
        val numberOfRows = reader.getStripes.asScala.map(_.getNumberOfRows).sum
        if (numberOfRows > 0) {
          break()
        }
      }
    }

    if (index > 0) {
      val tmp = compactFromPaths(0)
      compactFromPaths(0) = compactFromPaths(index)
      compactFromPaths(index) = tmp
    }
    compactFromPaths
  }

  private def isAllFilesEmpty(
      conf: Configuration,
      compactFailedSourcePaths: Array[Path]): Boolean = {
    val readOptions = OrcFile.readerOptions(conf)
    compactFailedSourcePaths.foreach { compactFailed =>
      val reader = OrcFile.createReader(compactFailed, readOptions)
      val numberOfRows = reader.getStripes.asScala.map(_.getNumberOfRows).sum
      if (numberOfRows != 0) {
        return false
      }
    }
    true
  }
}
