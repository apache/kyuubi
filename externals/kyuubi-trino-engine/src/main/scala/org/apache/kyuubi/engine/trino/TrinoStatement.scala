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

package org.apache.kyuubi.engine.trino

import java.util.OptionalDouble
import java.util.concurrent.{Executors, TimeUnit}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

import com.google.common.base.Verify
import io.trino.client.ClientSession
import io.trino.client.ClientTypeSignature
import io.trino.client.Column
import io.trino.client.StatementClient
import io.trino.client.StatementClientFactory

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_TRINO_SHOW_PROGRESS
import org.apache.kyuubi.config.KyuubiConf.ENGINE_TRINO_SHOW_PROGRESS_DEBUG
import org.apache.kyuubi.engine.trino.TrinoConf.DATA_PROCESSING_POOL_SIZE
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.util.ThreadUtils

/**
 * Trino client communicate with trino cluster.
 */
class TrinoStatement(
    trinoContext: TrinoContext,
    kyuubiConf: KyuubiConf,
    sql: String,
    operationLog: Option[OperationLog]) extends Logging {

  private val defaultSchema: List[Column] =
    List(new Column("Result", "VARCHAR", new ClientTypeSignature("VARCHAR")))

  private lazy val trino = StatementClientFactory
    .newStatementClient(trinoContext.httpClient, trinoContext.clientSession.get, sql)

  private lazy val dataProcessingPoolSize = kyuubiConf.get(DATA_PROCESSING_POOL_SIZE)
  private lazy val showProcess = kyuubiConf.get(ENGINE_TRINO_SHOW_PROGRESS)
  private lazy val showDebug = kyuubiConf.get(ENGINE_TRINO_SHOW_PROGRESS_DEBUG)

  private val timer =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("Trino-Status-Printer", false)
  private var lastStats: OptionalDouble = OptionalDouble.empty()

  implicit val ec: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(dataProcessingPoolSize))

  def getTrinoClient: StatementClient = trino

  def getCurrentCatalog: String = trinoContext.clientSession.get.getCatalog

  def getCurrentDatabase: String = trinoContext.clientSession.get.getSchema

  def getColumns: List[Column] = {
    while (trino.isRunning) {
      val results = trino.currentStatusInfo()
      val columns = results.getColumns()
      if (columns != null) {
        info(s"Execute with Trino query id: ${results.getId}")
        if (columns.isEmpty) {
          return defaultSchema
        }
        return columns.asScala.toList
      }
      trino.advance()
    }
    Verify.verify(trino.isFinished())
    val finalStatus = trino.finalStatusInfo()
    if (finalStatus.getError == null) {
      throw KyuubiSQLException(s"Query has no columns (#${finalStatus.getId})")
    } else {
      throw KyuubiSQLException(
        s"Query failed (#${finalStatus.getId}): ${finalStatus.getError.getMessage}")
    }
  }

  def execute(): Iterator[List[Any]] = {
    Iterator.continually {
      @tailrec
      def getData(): (Boolean, List[List[Any]]) = {
        if (trino.isRunning) {
          val data = trino.currentData().getData()
          trino.advance()
          if (data != null) {
            (true, data.asScala.toList.map(_.asScala.toList))
          } else {
            getData()
          }
        } else {
          timer.shutdown()
          Verify.verify(trino.isFinished)
          if (operationLog.isDefined && showProcess) {
            TrinoStatusPrinter.printStatusInfo(trino, operationLog.get, showDebug)
          }
          val finalStatus = trino.finalStatusInfo()
          if (finalStatus.getError() != null) {
            throw KyuubiSQLException(
              s"Query ${finalStatus.getId} failed: ${finalStatus.getError.getMessage}")
          }
          updateTrinoContext()
          (false, List[List[Any]]())
        }
      }
      getData()
    }
      .takeWhile(_._1)
      .flatMap(_._2)
  }

  def updateTrinoContext(): Unit = {
    val session = trinoContext.clientSession.get

    var builder = ClientSession.builder(session)
    // update catalog and schema
    if (trino.getSetCatalog.isPresent || trino.getSetSchema.isPresent) {
      builder = builder
        .catalog(trino.getSetCatalog.orElse(session.getCatalog))
        .schema(trino.getSetSchema.orElse(session.getSchema))
    }

    // update path if present
    if (trino.getSetPath.isPresent) {
      builder = builder.path(trino.getSetPath.get)
    }

    // update session properties if present
    if (!trino.getSetSessionProperties.isEmpty || !trino.getResetSessionProperties.isEmpty) {
      val properties = session.getProperties.asScala.clone()
      properties ++= trino.getSetSessionProperties.asScala
      properties --= trino.getResetSessionProperties.asScala
      builder = builder.properties(properties.asJava)
    }

    trinoContext.clientSession.set(builder.build())
  }
  def printStatusInfo(): Unit = {
    if (operationLog.isDefined && showProcess) {
      timer.scheduleWithFixedDelay(
        () => {
          if (trino.isRunning) {
            lastStats =
              TrinoStatusPrinter.printStatusInfo(trino, operationLog.get, showDebug, lastStats)
          }
        },
        500L,
        kyuubiConf.get(KyuubiConf.ENGINE_TRINO_SHOW_PROGRESS_UPDATE_INTERVAL),
        TimeUnit.MILLISECONDS)
    }
  }

  def stopPrinter(): Unit = {
    timer.shutdown()
  }
}

object TrinoStatement {
  def apply(
      trinoContext: TrinoContext,
      kyuubiConf: KyuubiConf,
      sql: String,
      operationLog: Option[OperationLog] = None): TrinoStatement = {
    new TrinoStatement(trinoContext, kyuubiConf, sql, operationLog)
  }
}
