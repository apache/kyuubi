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

package org.apache.kyuubi.server.trino.api

import java.net.URI
import java.time.ZoneId
import java.util.{Locale, Optional}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import com.google.common.base.Verify
import io.airlift.units.Duration
import io.trino.client.{ClientSession, StatementClient, StatementClientFactory}
import okhttp3.OkHttpClient

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException, TrinoRestFrontendTestHelper}

class TrinoClientApiSuite extends KyuubiFunSuite with TrinoRestFrontendTestHelper {

  private val httpClient =
    new OkHttpClient.Builder()
      .readTimeout(5, TimeUnit.MINUTES)
      .build()
  private lazy val clientSession =
    new AtomicReference[ClientSession](createTestClientSession(baseUri))

  test("submit query with trino client api") {
    val trino = getTrinoStatementClient("select 1")
    val result = execute(trino)
    val sessionId = trino.getSetSessionProperties.asScala.get(Query.KYUUBI_SESSION_ID)
    assert(result == List(List(1)))

    updateClientSession(trino)

    val trino1 = getTrinoStatementClient("set k=v")
    val result1 = execute(trino1)
    val sessionId1 = trino1.getSetSessionProperties.asScala.get(Query.KYUUBI_SESSION_ID)
    assert(result1 == List(List("k", "v")))
    assert(sessionId == sessionId1)

    updateClientSession(trino)

    val trino2 = getTrinoStatementClient("set k")
    val result2 = execute(trino2)
    val sessionId2 = trino2.getSetSessionProperties.asScala.get(Query.KYUUBI_SESSION_ID)
    assert(result2 == List(List("k", "v")))
    assert(sessionId == sessionId2)

    trino.close()
  }

  private def updateClientSession(trino: StatementClient): Unit = {
    val session = clientSession.get

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
    clientSession.set(builder.build())
  }

  private def execute(trino: StatementClient): List[List[Any]] = {
    @tailrec
    def getData(trino: StatementClient): (Boolean, List[List[Any]]) = {
      if (trino.isRunning) {
        val data = trino.currentData().getData()
        trino.advance()
        if (data != null) {
          (true, data.asScala.toList.map(_.asScala.toList))
        } else {
          getData(trino)
        }
      } else {
        Verify.verify(trino.isFinished)
        val finalStatus = trino.finalStatusInfo()
        if (finalStatus.getError() != null) {
          throw KyuubiSQLException(
            s"Query ${finalStatus.getId} failed: ${finalStatus.getError.getMessage}")
        }
        (false, List[List[Any]]())
      }
    }

    Iterator.continually(getData(trino)).takeWhile(_._1).flatMap(_._2).toList
  }

  private def getTrinoStatementClient(sql: String): StatementClient = {
    StatementClientFactory.newStatementClient(httpClient, clientSession.get, sql)
  }

  private def createTestClientSession(connectUrl: URI): ClientSession = {
    ClientSession.builder()
      .server(connectUrl)
      .principal(Optional.of("kyuubi_test"))
      .user(Optional.of("test_user"))
      .source("kyuubi")
      .traceToken(Optional.of("test_token_tracing"))
      .clientInfo("test_client_info")
      .catalog("test_catalog")
      .schema("test_schema")
      .timeZone(ZoneId.systemDefault())
      .locale(Locale.getDefault)
      .properties(Map[String, String](
        "test_property_key0" -> "test_property_value0",
        "test_property_key1" -> "test_propert_value1").asJava)
      .preparedStatements(Map[String, String](
        "test_statement_key0" -> "select 1",
        "test_statement_key1" -> "select 2").asJava)
      .clientRequestTimeout(new Duration(2, TimeUnit.MINUTES))
      .compressionDisabled(true)
      .build()
  }

}
