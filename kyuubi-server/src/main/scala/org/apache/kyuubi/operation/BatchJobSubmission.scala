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
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.{TColumn, TColumnDesc, TPrimitiveTypeEntry, TRow, TRowSet, TStringColumn, TTableSchema, TTypeDesc, TTypeEntry, TTypeId}

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.ProcBuilder
import org.apache.kyuubi.engine.spark.SparkBatchProcessBuilder
import org.apache.kyuubi.operation.FetchOrientation.{FETCH_FIRST, FETCH_NEXT, FETCH_PRIOR, FetchOrientation}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.server.api.v1.{BatchRequest, BatchType}
import org.apache.kyuubi.session.KyuubiBatchSessionImpl
import org.apache.kyuubi.util.ThreadUtils

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
  private val applicationCheckExecutor =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor(s"application-checker-${session.batchId}")

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

    if (!shouldRunAsync) getBackgroundHandle.get()
  }

  private def submitBatchJob(): Unit = {
    builder = BatchType.withName(batchRequest.batchType.toUpperCase(Locale.ROOT)) match {
      case BatchType.SPARK =>
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
      startApplicationChecker()
      process.waitFor()
      if (process.exitValue() != 0) {
        throw new KyuubiException(s"Process exit with value ${process.exitValue()}")
      }
    } finally {
      builder.close()
    }
  }

  private def startApplicationChecker(): Unit = {
    val checker = new Runnable {
      override def run(): Unit = {
        if (appIdAndUrl.isEmpty) {
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
        }
      }
    }

    applicationCheckExecutor.scheduleAtFixedRate(
      checker,
      applicationCheckInterval,
      applicationCheckInterval,
      TimeUnit.MILLISECONDS)
  }

  override def getResultSetSchema: TTableSchema = {
    val tAppIdColumnDesc = new TColumnDesc()
    tAppIdColumnDesc.setColumnName("ApplicationId")
    val appIdDesc = new TTypeDesc
    appIdDesc.addToTypes(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.STRING_TYPE)))
    tAppIdColumnDesc.setTypeDesc(appIdDesc)
    tAppIdColumnDesc.setPosition(0)

    val tUrlColumnDesc = new TColumnDesc()
    tUrlColumnDesc.setColumnName("URL")
    val urlDesc = new TTypeDesc
    urlDesc.addToTypes(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.STRING_TYPE)))
    tUrlColumnDesc.setTypeDesc(urlDesc)
    tUrlColumnDesc.setPosition(1)

    val schema = new TTableSchema()
    schema.addToColumns(tAppIdColumnDesc)
    schema.addToColumns(tUrlColumnDesc)
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
      new TRowSet(0, new JArrayList[TRow](0))
    }
  }

  override def close(): Unit = {
    if (!isClosedOrCanceled) {
      if (builder != null) {
        builder.close()
      }
      ThreadUtils.shutdown(applicationCheckExecutor)
    }
    super.close()
  }
}
