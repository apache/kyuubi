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

package yaooqinn.kyuubi.operation

import java.io.{BufferedReader, File, FileInputStream, FileNotFoundException, FileOutputStream, InputStreamReader, IOException, PrintStream}
import java.util.ArrayList

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.IOUtils
import org.apache.kyuubi.Logging
import org.apache.spark.sql.Row

import yaooqinn.kyuubi.KyuubiSQLException

class LogFile private (
    file: File,
    private var reader: Option[BufferedReader],
    writer: PrintStream,
    @volatile private var isRemoved: Boolean = false) extends Logging {

  def this(file: File) = {
    this(file,
      LogFile.createReader(file, isRemoved = false),
      new PrintStream(new FileOutputStream(file)))
  }

  private def resetReader(): Unit = {
    reader.foreach(IOUtils.closeStream)
    reader = None
  }

  private def readResults(nLines: Long): Seq[Row] = {
    reader = reader.orElse(LogFile.createReader(file, isRemoved))

    val logs = new ArrayList[Row]()
    reader.foreach { r =>
      var i = 1
      try {
        var line: String = r.readLine()
        while ((i < nLines || nLines <= 0) && line != null) {
          logs.add(Row(line))
          line = r.readLine()
          i += 1
        }
      } catch {
        case e: FileNotFoundException =>
          val operationHandle = file.getName
          val path = file.getAbsolutePath
          val msg = if (isRemoved) {
            s"Operation[$operationHandle] has been closed and the log file $path has been removed"
          } else {
            s"Operation[$operationHandle] Log file $path is not found"
          }
          throw new KyuubiSQLException(msg, e)
      }
    }
    logs.asScala
  }

  /**
   * Read to log file line by line
   *
   * @param isFetchFirst  reset the BufferReader, if fetching from the beginning of the file if true
   * @param maxRows      maximum result number can reach
   * @return a row seq of lines from the log file
   */
  def read(isFetchFirst: Boolean, maxRows: Long): Seq[Row] = synchronized {
    if (isFetchFirst) resetReader()

    readResults(maxRows)
  }

  /**
   * write log to the operation log file
   */
  def write(msg: String): Unit = {
    writer.print(msg)
  }


  def close(): Unit = synchronized {
    try {
      reader.foreach(_.close())
      writer.close()
      if (!isRemoved) {
        FileUtils.forceDelete(file)
        isRemoved = true
      }
    } catch {
      case e: IOException =>
        error(s"Failed to remove corresponding log file of operation: ${file.getName}", e)
    }
  }
}

object LogFile {

  def createReader(file: File, isRemoved: Boolean): Option[BufferedReader] = try {
    Option(new BufferedReader(new InputStreamReader(new FileInputStream(file))))
  } catch {
    case e: FileNotFoundException =>
      val operationHandle = file.getName
      val path = file.getAbsolutePath
      val msg = if (isRemoved) {
        s"Operation[$operationHandle] has been closed and the log file $path has been removed"
      } else {
        s"Operation[$operationHandle] Log file $path is not found"
      }
      throw new KyuubiSQLException(msg, e)
  }
}
