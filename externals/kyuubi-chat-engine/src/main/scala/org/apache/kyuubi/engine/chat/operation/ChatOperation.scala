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
package org.apache.kyuubi.engine.chat.operation

import org.apache.kyuubi.{KyuubiSQLException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.chat.schema.{ChatTRowSetGenerator, SchemaHelper}
import org.apache.kyuubi.engine.chat.schema.ChatTRowSetGenerator.COL_STRING_TYPE
import org.apache.kyuubi.operation.{AbstractOperation, FetchIterator, OperationState}
import org.apache.kyuubi.operation.FetchOrientation.{FETCH_FIRST, FETCH_NEXT, FETCH_PRIOR, FetchOrientation}
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

abstract class ChatOperation(session: Session) extends AbstractOperation(session) {

  protected var iter: FetchIterator[Array[String]] = _

  protected lazy val conf: KyuubiConf = session.sessionManager.getConf

  override def getNextRowSetInternal(
      order: FetchOrientation,
      rowSetSize: Int): TFetchResultsResp = {
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    order match {
      case FETCH_NEXT =>
        iter.fetchNext()
      case FETCH_PRIOR =>
        iter.fetchPrior(rowSetSize)
      case FETCH_FIRST =>
        iter.fetchAbsolute(0)
    }

    val taken = iter.take(rowSetSize).map(_.toSeq)
    val resultRowSet = new ChatTRowSetGenerator().toTRowSet(
      taken.toSeq,
      Seq(COL_STRING_TYPE),
      getProtocolVersion)
    resultRowSet.setStartRowOffset(iter.getPosition)
    val resp = new TFetchResultsResp(OK_STATUS)
    resp.setResults(resultRowSet)
    resp.setHasMoreRows(false)
    resp
  }

  override def cancel(): Unit = {
    cleanup(OperationState.CANCELED)
  }

  override def close(): Unit = {
    cleanup(OperationState.CLOSED)
  }

  protected def onError(cancel: Boolean = false): PartialFunction[Throwable, Unit] = {
    // We should use Throwable instead of Exception since `java.lang.NoClassDefFoundError`
    // could be thrown.
    case e: Throwable =>
      withLockRequired {
        val errMsg = Utils.stringifyException(e)
        if (state == OperationState.TIMEOUT) {
          val ke = KyuubiSQLException(s"Timeout operating $opType: $errMsg")
          setOperationException(ke)
          throw ke
        } else if (isTerminalState(state)) {
          setOperationException(KyuubiSQLException(errMsg))
          warn(s"Ignore exception in terminal state with $statementId: $errMsg")
        } else {
          error(s"Error operating $opType: $errMsg", e)
          val ke = KyuubiSQLException(s"Error operating $opType: $errMsg", e)
          setOperationException(ke)
          setState(OperationState.ERROR)
          throw ke
        }
      }
  }

  override protected def beforeRun(): Unit = {
    setState(OperationState.PENDING)
    setHasResultSet(true)
  }

  override protected def afterRun(): Unit = {}

  override def getResultSetMetadata: TGetResultSetMetadataResp = {
    val tTableSchema = SchemaHelper.stringTTableSchema("reply")
    val resp = new TGetResultSetMetadataResp
    resp.setSchema(tTableSchema)
    resp.setStatus(OK_STATUS)
    resp
  }

  override def shouldRunAsync: Boolean = false
}
