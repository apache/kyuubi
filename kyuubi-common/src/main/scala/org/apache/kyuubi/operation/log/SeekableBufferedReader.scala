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

package org.apache.kyuubi.operation.log

import java.io.{BufferedReader, Closeable, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

/**
 * A seekable buffer reader, if the previous file path lines do not satisfy the limit size
 * the reader will fetch line from next file path.
 *
 * Note that, this reader is always read forwards so call side should always new instance if
 * want to replay the lines which has been fetched.
 */
class SeekableBufferedReader(paths: Seq[Path]) extends Closeable {

  private val bufferedReaders: Seq[BufferedReader] = paths.map { path =>
    Files.newBufferedReader(path, StandardCharsets.UTF_8)
  }

  private var linePos = 0L
  private var readerIndex = 0
  private val numReaders = bufferedReaders.length
  private var currentReader = bufferedReaders.head
  private var currentValue: String = _

  private def nextLine(): Unit = {
    currentValue = currentReader.readLine()
    while (currentValue == null && readerIndex < numReaders - 1) {
      readerIndex += 1
      currentReader = bufferedReaders(readerIndex)
      currentValue = currentReader.readLine()
    }
    if (currentValue != null) {
      linePos += 1
    }
  }

  /**
   * @param from include
   * @param limit exclude
   */
  def readLine(from: Long, limit: Long): Iterator[String] = {
    if (from < 0) throw new IOException("Negative seek offset")

    new Iterator[String] {
      private var numLines = 0L
      override def hasNext: Boolean = {
        if (numLines >= limit) {
          false
        } else {
          nextLine()
          while (linePos <= from && currentValue != null) {
            nextLine()
          }
          numLines += 1
          currentValue != null
        }
      }

      override def next(): String = {
        currentValue
      }
    }
  }

  override def close(): Unit = {
    bufferedReaders.foreach(_.close())
  }
}
