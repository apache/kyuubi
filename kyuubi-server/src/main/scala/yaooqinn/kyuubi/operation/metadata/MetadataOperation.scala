/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.operation.metadata

import java.security.PrivilegedExceptionAction

import org.apache.commons.lang3.StringUtils
import org.apache.kyuubi.KyuubiSQLException
import org.apache.spark.KyuubiSparkUtil
import yaooqinn.kyuubi.cli.FetchOrientation
import yaooqinn.kyuubi.operation._
import yaooqinn.kyuubi.schema.{RowSet, RowSetBuilder}
import yaooqinn.kyuubi.session.KyuubiSession

abstract class MetadataOperation(session: KyuubiSession, opType: OperationType)
  extends AbstractOperation(session, opType) {

  setHasResultSet(true)

  override def cancel(): Unit = {
    setState(CANCELED)
    throw new UnsupportedOperationException("MetadataOperation.cancel()")
  }

  override def close(): Unit = {
    setState(CLOSED)
    cleanupOperationLog()
  }

  /**
   * Convert wildchars and escape sequence from JDBC format to datanucleous/regex
   */
  protected def convertIdentifierPattern(pattern: String, datanucleusFormat: Boolean): String = {
    if (pattern == null) {
      convertPattern("%", datanucleusFormat = true)
    } else {
      convertPattern(pattern, datanucleusFormat)
    }
  }

  /**
   * Convert wildchars and escape sequence of schema pattern from JDBC format to datanucleous/regex
   * The schema pattern treats empty string also as wildchar
   */
  protected def convertSchemaPattern(pattern: String): String = {
    if (StringUtils.isEmpty(pattern)) {
      convertPattern("%", datanucleusFormat = true)
    } else {
      convertPattern(pattern, datanucleusFormat = true)
    }
  }

  private def convertPattern(pattern: String, datanucleusFormat: Boolean): String = {
    val wStr = if (datanucleusFormat) "*" else ".*"
    pattern
      .replaceAll("([^\\\\])%", "$1" + wStr)
      .replaceAll("\\\\%", "%")
      .replaceAll("^%", wStr)
      .replaceAll("([^\\\\])_", "$1.")
      .replaceAll("\\\\_", "_")
      .replaceAll("^_", ".")
  }

  protected def execute(block: => Unit): Unit = {
    setState(RUNNING)
    try {
      session.ugi.doAs(new PrivilegedExceptionAction[Unit] {
        override def run(): Unit = block
      })
      setState(FINISHED)
    } catch {
      case e: Exception =>
        setState(ERROR)
        throw KyuubiSQLException(KyuubiSparkUtil.findCause(e))
    }
  }

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Long): RowSet = {
    assertState(FINISHED)
    validateDefaultFetchOrientation(order)
    val taken = iter.take(rowSetSize.toInt)
    RowSetBuilder.create(getResultSetSchema, taken.toSeq, getProtocolVersion)
  }

}
