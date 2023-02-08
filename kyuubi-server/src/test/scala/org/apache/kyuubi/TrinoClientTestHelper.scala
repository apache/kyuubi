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

package org.apache.kyuubi

import java.net.URI
import java.time.ZoneId
import java.util.{Locale, Optional}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import io.airlift.units.Duration
import io.trino.client.{ClientSelectedRole, ClientSession, StatementClient, StatementClientFactory}
import okhttp3.OkHttpClient

trait TrinoClientTestHelper extends RestFrontendTestHelper {

  override def afterAll(): Unit = {
    super.afterAll()
  }

  private val httpClient = new OkHttpClient.Builder().build()

  protected val clientSession = createClientSession(baseUri: URI)

  def getTrinoStatementClient(sql: String): StatementClient = {
    StatementClientFactory.newStatementClient(httpClient, clientSession, sql)
  }

  def createClientSession(connectUrl: URI): ClientSession = {
    new ClientSession(
      connectUrl,
      "kyuubi_test",
      Optional.of("test_user"),
      "kyuubi",
      Optional.of("test_token_tracing"),
      Set[String]().asJava,
      "test_client_info",
      "test_catalog",
      "test_schema",
      "test_path",
      ZoneId.systemDefault(),
      Locale.getDefault,
      Map[String, String](
        "test_resource_key0" -> "test_resource_value0",
        "test_resource_key1" -> "test_resource_value1").asJava,
      Map[String, String](
        "test_property_key0" -> "test_property_value0",
        "test_property_key1" -> "test_propert_value1").asJava,
      Map[String, String](
        "test_statement_key0" -> "select 1",
        "test_statement_key1" -> "select 2").asJava,
      Map[String, ClientSelectedRole](
        "test_role_key0" -> ClientSelectedRole.valueOf("ROLE"),
        "test_role_key2" -> ClientSelectedRole.valueOf("ALL")).asJava,
      Map[String, String](
        "test_credentials_key0" -> "test_credentials_value0",
        "test_credentials_key1" -> "test_credentials_value1").asJava,
      "test_transaction_id",
      new Duration(2, TimeUnit.MINUTES),
      true)

  }

}
