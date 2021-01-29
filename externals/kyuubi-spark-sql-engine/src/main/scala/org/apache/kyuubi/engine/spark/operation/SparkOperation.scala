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

package org.apache.kyuubi.engine.spark.operation

import org.apache.commons.lang3.StringUtils
import org.apache.hive.service.rpc.thrift.{TRowSet, TTableSchema}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.operation.{AbstractOperation, OperationState}
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.operation.OperationType.OperationType
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.schema.{RowSet, SchemaHelper}
import org.apache.kyuubi.session.Session

abstract class SparkOperation(spark: SparkSession, opType: OperationType, session: Session)
  extends AbstractOperation(opType, session) {

  protected var iter: Iterator[Row] = _

  protected final val operationLog: OperationLog =
    OperationLog.createOperationLog(session.handle, getHandle)
  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  protected def resultSchema: StructType

  protected def cleanup(targetState: OperationState): Unit = synchronized {
    if (!isTerminalState(state)) {
      setState(targetState)
      spark.sparkContext.cancelJobGroup(statementId)
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

  /**
   * Convert wildcards and escape sequence of schema pattern from JDBC format to datanucleous/regex
   * The schema pattern treats empty string also as wildcard
   */
  protected def convertSchemaPattern(pattern: String, datanucleusFormat: Boolean = true): String = {
    if (StringUtils.isEmpty(pattern)) {
      convertPattern("%", datanucleusFormat)
    } else {
      convertPattern(pattern, datanucleusFormat)
    }
  }

  /**
   * Convert wildcards and escape sequence from JDBC format to datanucleous/regex
   */
  protected def convertIdentifierPattern(pattern: String, datanucleusFormat: Boolean): String = {
    if (pattern == null) {
      convertPattern("%", datanucleusFormat)
    } else {
      convertPattern(pattern, datanucleusFormat)
    }
  }

  protected def onError(cancel: Boolean = false): PartialFunction[Throwable, Unit] = {
    case e: Exception =>
      if (cancel) spark.sparkContext.cancelJobGroup(statementId)
      state.synchronized {
        val errMsg = KyuubiSQLException.stringifyException(e)
        if (isTerminalState(state)) {
          warn(s"Ignore exception in terminal state with $statementId: $errMsg")
        } else {
          setState(OperationState.ERROR)
          val ke = KyuubiSQLException(s"Error operating $opType: $errMsg", e)
          setOperationException(ke)
          throw ke
        }
      }
  }

  override protected def beforeRun(): Unit = {
    Thread.currentThread().setContextClassLoader(spark.sharedState.jarClassLoader)
    setHasResultSet(true)
    setState(OperationState.RUNNING)
    OperationLog.setCurrentOperationLog(operationLog)
  }

  override protected def afterRun(): Unit = {
    state.synchronized {
      if (!isTerminalState(state)) {
        setState(OperationState.FINISHED)
      }
    }
    OperationLog.removeCurrentOperationLog()
  }

  override def cancel(): Unit = {
    cleanup(OperationState.CANCELED)
  }

  override def close(): Unit = {
    cleanup(OperationState.CLOSED)
    getOperationLog.foreach(_.close())
  }

  override def getResultSetSchema: TTableSchema = SchemaHelper.toTTableSchema(resultSchema)

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Int): TRowSet = {
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    val taken = iter.take(rowSetSize)
    RowSet.toTRowSet(taken.toList, resultSchema, getProtocolVersion)
  }

  override def shouldRunAsync: Boolean = false
}
