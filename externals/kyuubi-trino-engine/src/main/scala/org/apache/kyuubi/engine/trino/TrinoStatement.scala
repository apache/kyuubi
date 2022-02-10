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

import java.util.ArrayList
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executors

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration
import scala.concurrent.duration.Duration

import com.google.common.base.Verify
import io.trino.client.ClientSession
import io.trino.client.Column
import io.trino.client.StatementClient
import io.trino.client.StatementClientFactory

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.trino.TrinoConf.DATA_PROCESSING_POOL_SIZE
import org.apache.kyuubi.engine.trino.TrinoStatement._

/**
 * Trino client communicate with trino cluster.
 */
class TrinoStatement(trinoContext: TrinoContext, kyuubiConf: KyuubiConf, sql: String) {

  private lazy val trino = StatementClientFactory
    .newStatementClient(trinoContext.httpClient, trinoContext.clientSession.get, sql)

  private lazy val dataProcessingPoolSize = kyuubiConf.get(DATA_PROCESSING_POOL_SIZE)

  implicit val ec: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(dataProcessingPoolSize))

  def getTrinoClient: StatementClient = trino

  def getCurrentDatabase: String = trinoContext.clientSession.get.getSchema

  def getColumns: List[Column] = {
    while (trino.isRunning) {
      val results = trino.currentStatusInfo()
      val columns = results.getColumns()
      if (columns != null) {
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

  /**
   * Execute sql and return ResultSet.
   */
  def execute(): Iterable[List[Any]] = {
    val rowQueue = new ArrayBlockingQueue[List[Any]](MAX_QUEUED_ROWS)

    val dataProcessing = Future[Unit] {
      while (trino.isRunning) {
        val data = trino.currentData().getData()
        if (data != null) {
          data.asScala.map(_.asScala.toList)
            .foreach(e => putOrThrow(rowQueue, e))
        }
        trino.advance()
      }
    }
    dataProcessing.onComplete {
      case _ => putOrThrow(rowQueue, END_TOKEN)
    }

    val rowBuffer = new ArrayList[List[Any]](MAX_BUFFERED_ROWS)
    var bufferStart = System.nanoTime()
    val result = ArrayBuffer[List[Any]]()

    var getDataEnd = false
    while (!dataProcessing.isCompleted && !getDataEnd) {
      val atEnd = drainDetectingEnd(rowQueue, rowBuffer, MAX_BUFFERED_ROWS, END_TOKEN)
      if (!atEnd) {
        // Flush if needed
        if (rowBuffer.size() >= MAX_BUFFERED_ROWS ||
          Duration.fromNanos(bufferStart).compareTo(MAX_BUFFER_TIME) >= 0) {
          result ++= rowBuffer.asScala
          rowBuffer.clear()
          bufferStart = System.nanoTime()
        }

        val row = rowQueue.poll(MAX_BUFFER_TIME.toMillis, duration.MILLISECONDS)
        row match {
          case END_TOKEN => getDataEnd = true
          case null =>
          case _ => rowBuffer.add(row)
        }
      }
    }
    if (!rowQueue.isEmpty()) {
      drainDetectingEnd(rowQueue, rowBuffer, Integer.MAX_VALUE, END_TOKEN)
    }
    result ++= rowBuffer.asScala

    val finalStatus = trino.finalStatusInfo()
    if (finalStatus.getError() != null) {
      throw KyuubiSQLException(
        s"Query ${finalStatus.getId} failed: ${finalStatus.getError.getMessage}")
    }
    updateTrinoContext()

    result
  }

  def updateTrinoContext(): Unit = {
    val session = trinoContext.clientSession.get

    var builder = ClientSession.builder(session)
    // update catalog and schema
    if (trino.getSetCatalog.isPresent || trino.getSetSchema.isPresent) {
      builder = builder
        .withCatalog(trino.getSetCatalog.orElse(session.getCatalog))
        .withSchema(trino.getSetSchema.orElse(session.getSchema))
    }

    // update path if present
    if (trino.getSetPath.isPresent) {
      builder = builder.withPath(trino.getSetPath.get)
    }

    // update session properties if present
    if (!trino.getSetSessionProperties.isEmpty || !trino.getResetSessionProperties.isEmpty) {
      val properties = session.getProperties.asScala.clone()
      properties ++= trino.getSetSessionProperties.asScala
      properties --= trino.getResetSessionProperties.asScala
      builder = builder.withProperties(properties.asJava)
    }

    trinoContext.clientSession.set(builder.build())
  }

  private def drainDetectingEnd(
      rowQueue: ArrayBlockingQueue[List[Any]],
      buffer: ArrayList[List[Any]],
      maxBufferSize: Int,
      endToken: List[Any]): Boolean = {
    val drained = rowQueue.drainTo(buffer, maxBufferSize - buffer.size)
    if (drained > 0 && buffer.get(buffer.size() - 1) == endToken) {
      buffer.remove(buffer.size() - 1);
      true
    } else {
      false
    }
  }

  private def putOrThrow(rowQueue: ArrayBlockingQueue[List[Any]], e: List[Any]): Unit = {
    try {
      rowQueue.put(e)
    } catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new RuntimeException(e)
    }
  }
}

object TrinoStatement {
  final private val MAX_QUEUED_ROWS = 50000
  final private val MAX_BUFFERED_ROWS = 10000
  final private val MAX_BUFFER_TIME = Duration(3, duration.SECONDS)
  final private val END_TOKEN = List[Any]()

  def apply(trinoContext: TrinoContext, kyuubiConf: KyuubiConf, sql: String): TrinoStatement = {
    new TrinoStatement(trinoContext, kyuubiConf, sql)
  }
}
