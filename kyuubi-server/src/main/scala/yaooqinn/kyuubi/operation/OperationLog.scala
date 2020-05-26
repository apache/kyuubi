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

import java.io.File

import org.apache.kyuubi.Logging
import org.apache.spark.sql.Row

class OperationLog(logFile: LogFile) extends Logging {

  def this(file: File) = this(new LogFile(file))

  def write(msg: String): Unit = logFile.write(msg)

  def read(isFetchFirst: Boolean, maxRows: Long): Seq[Row] = {
    logFile.read(isFetchFirst, maxRows)
  }

  def close(): Unit = logFile.close()
}

object OperationLog {
  private final val OPERATION_LOG = new ThreadLocal[OperationLog] {
    override def initialValue(): OperationLog = null
  }

  def setCurrentOperationLog(operationLog: OperationLog): Unit = {
    OPERATION_LOG.set(operationLog)
  }

  def getCurrentOperationLog: OperationLog = OPERATION_LOG.get()

  def removeCurrentOperationLog(): Unit = OPERATION_LOG.remove()
}
