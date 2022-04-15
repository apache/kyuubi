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

package org.apache.kyuubi.operation

import java.nio.ByteBuffer
import java.util.{ArrayList => JArrayList, Locale}

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.ProcBuilder
import org.apache.kyuubi.engine.spark.SparkBatchProcessBuilder
import org.apache.kyuubi.operation.FetchOrientation.{FETCH_FIRST, FETCH_NEXT, FETCH_PRIOR, FetchOrientation}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.server.api.v1.BatchRequest
import org.apache.kyuubi.session.KyuubiBatchSessionImpl
import org.apache.kyuubi.util.ThriftUtils

class BatchJobSubmission(session: KyuubiBatchSessionImpl, batchRequest: BatchRequest)
  extends KyuubiOperation(OperationType.UNKNOWN_OPERATION, session) {

  override def statement: String = "BATCH_JOB_SUBMISSION"

  override def shouldRunAsync: Boolean = true

  private lazy val _operationLog = OperationLog.createOperationLog(session, getHandle)

  private var builder: ProcBuilder = _

  @volatile
  private[kyuubi] var appIdAndUrl: Option[(String, String)] = None

  private var resultFetched: Boolean = _

  private val applicationCheckInterval =
    session.sessionConf.get(KyuubiConf.BATCH_APPLICATION_CHECK_INTERVAL)

  override def getOperationLog: Option[OperationLog] = Option(_operationLog)

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(_operationLog)
    setHasResultSet(false)
    setState(OperationState.PENDING)
  }

  override protected def afterRun(): Unit = {
    OperationLog.removeCurrentOperationLog()
  }

  override protected def runInternal(): Unit = {
    val asyncOperation: Runnable = () => {
      setState(OperationState.RUNNING)
      try {
        submitBatchJob()
        setState(OperationState.FINISHED)
      } catch onError()
    }
    try {
      val opHandle = session.sessionManager.submitBackgroundOperation(asyncOperation)
      setBackgroundHandle(opHandle)
    } catch onError("submitting batch job submission operation in background, request rejected")
  }

  private def submitBatchJob(): Unit = {
    builder = Option(batchRequest.batchType).map(_.toUpperCase(Locale.ROOT)) match {
      case Some("SPARK") =>
        new SparkBatchProcessBuilder(
          session.user,
          session.sessionConf,
          session.batchId,
          batchRequest,
          getOperationLog)

      case _ =>
        throw new UnsupportedOperationException(s"Batch type ${batchRequest.batchType} unsupported")
    }

    try {
      info(s"Submitting ${batchRequest.batchType} batch job: $builder")
      val process = builder.start
      while (appIdAndUrl.isEmpty) {
        try {
          builder match {
            case sparkBatchProcessBuilder: SparkBatchProcessBuilder =>
              sparkBatchProcessBuilder.getApplicationIdAndUrl() match {
                case Some(appInfo) => appIdAndUrl = Some(appInfo)
                case _ =>
              }

            case _ =>
          }
        } catch {
          case e: Exception => error(s"Failed to check batch application", e)
        }
        Thread.sleep(applicationCheckInterval)
      }
      process.waitFor()
      if (process.exitValue() != 0) {
        throw new KyuubiException(s"Process exit with value ${process.exitValue()}")
      }
    } finally {
      builder.close()
    }
  }

  override def getResultSetSchema: TTableSchema = {
    val schema = new TTableSchema()
    Seq("ApplicationId", "URL").zipWithIndex.foreach { case (colName, position) =>
      val tColumnDesc = new TColumnDesc()
      tColumnDesc.setColumnName(colName)
      val tTypeDesc = new TTypeDesc()
      tTypeDesc.addToTypes(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.STRING_TYPE)))
      tColumnDesc.setTypeDesc(tTypeDesc)
      tColumnDesc.setPosition(position)
      schema.addToColumns(tColumnDesc)
    }
    schema
  }

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Int): TRowSet = {
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    order match {
      case FETCH_NEXT => fetchNext()
      case FETCH_PRIOR => resultSet
      case FETCH_FIRST => resultSet
    }
  }

  private lazy val resultSet: TRowSet = {
    val tRow = new TRowSet(0, new JArrayList[TRow](1))
    val (appId, url) = appIdAndUrl.toSeq.unzip

    val tAppIdColumn = TColumn.stringVal(new TStringColumn(
      appId.asJava,
      ByteBuffer.allocate(0)))

    val tUrlColumn = TColumn.stringVal(new TStringColumn(
      url.asJava,
      ByteBuffer.allocate(0)))

    tRow.addToColumns(tAppIdColumn)
    tRow.addToColumns(tUrlColumn)
    tRow
  }

  private def fetchNext(): TRowSet = {
    if (!resultFetched) {
      resultFetched = true
      resultSet
    } else {
      ThriftUtils.EMPTY_ROW_SET
    }
  }

  override def close(): Unit = {
    if (!isClosedOrCanceled) {
      if (builder != null) {
        builder.close()
      }
    }
    super.close()
  }
}
