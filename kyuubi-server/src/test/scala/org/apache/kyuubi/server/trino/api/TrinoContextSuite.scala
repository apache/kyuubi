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

import java.time.ZoneId

import io.trino.client.ProtocolHeaders.TRINO_HEADERS

import org.apache.kyuubi.KyuubiFunSuite

class TrinoContextSuite extends KyuubiFunSuite {
  import TrinoContext._

  test("create trino request context with header") {
    val testHeader0 = Map(
      TRINO_HEADERS.requestUser -> List("requestUser"),
      TRINO_HEADERS.requestTimeZone -> List(ZoneId.systemDefault().getId),
      TRINO_HEADERS.requestTransactionId -> List("NONE"),
      TRINO_HEADERS.requestClientCapabilities -> List("requestClientCapabilities"),
      TRINO_HEADERS.requestSource -> List("requestSource"),
      TRINO_HEADERS.requestCatalog -> List("requestCatalog"),
      TRINO_HEADERS.requestSchema -> List("requestSchema"),
      TRINO_HEADERS.requestLanguage -> List("requestLanguage"),
      TRINO_HEADERS.requestTraceToken -> List("requestTraceToken"),
      TRINO_HEADERS.requestClientInfo -> List("requestClientInfo"),
      TRINO_HEADERS.requestClientTags -> List(
        "requestClientTag1,requestClientTag2,requestClientTag2"),
      TRINO_HEADERS.requestSession -> List(
        "",
        s"key0=${urlEncode("value0")}",
        s"key1=${urlEncode("value1")}",
        "badcase"),
      TRINO_HEADERS.requestPreparedStatement -> List(
        "badcase",
        s"key0=${urlEncode("select 1")}",
        s"key1=${urlEncode("select 2")}",
        ""))
    val expectedTrinoContext = new TrinoContext(
      user = "requestUser",
      timeZone = Some(ZoneId.systemDefault().getId),
      clientCapabilities = Some("requestClientCapabilities"),
      source = Some("requestSource"),
      catalog = Some("requestCatalog"),
      schema = Some("requestSchema"),
      language = Some("requestLanguage"),
      traceToken = Some("requestTraceToken"),
      clientInfo = Some("requestClientInfo"),
      clientTags = Set("requestClientTag1", "requestClientTag2"),
      session = Map("key0" -> "value0", "key1" -> "value1"),
      preparedStatement = Map("key0" -> "select 1", "key1" -> "select 2"))
    val actual = TrinoContext(testHeader0)
    assert(actual == expectedTrinoContext)
  }

}
