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

package org.apache.kyuubi.engine.spark.session

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.service.ServiceState._
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TGetInfoReq, TGetInfoType, TProtocolVersion, TStatusCode}

class SessionSuite extends WithSparkSQLEngine with HiveJDBCTestHelper {
  override def withKyuubiConf: Map[String, String] = {
    Map(
      ENGINE_SHARE_LEVEL.key -> "CONNECTION",
      ENGINE_SPARK_MAX_INITIAL_WAIT.key -> "0")
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    startSparkEngine()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    stopSparkEngine()
  }

  override protected def jdbcUrl: String =
    s"jdbc:hive2://${engine.frontendServices.head.connectionUrl}/;" +
      s"clientProtocolVersion=${TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11.getValue};" +
      "#spark.ui.enabled=false"

  private def assertSQLKeywords(value: String): Unit = {
    val keywords = value.split(",", -1).toSeq
    assert(
      Set("SELECT", "FROM", "WHERE").subsetOf(keywords.toSet),
      s"Expected Spark SQL keywords, but received: $value")
    assert(keywords.forall(_.matches("[A-Z_]+")))
    assert(keywords == keywords.sorted)
  }

  test("KYUUBI #5860: get Spark SQL keywords through Thrift GetInfo") {
    withSessionHandle { (client, handle) =>
      val request = new TGetInfoReq()
      request.setSessionHandle(handle)
      request.setInfoType(TGetInfoType.CLI_ODBC_KEYWORDS)
      val response = client.GetInfo(request)
      assert(response.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)
      assertSQLKeywords(response.getInfoValue.getStringValue)
    }
  }

  test("KYUUBI #5860: get Spark SQL keywords through JDBC metadata") {
    withJdbcStatement() { statement =>
      assertSQLKeywords(statement.getConnection.getMetaData.getSQLKeywords)
    }
  }

  test("release session if shared level is CONNECTION") {
    assert(engine.getServiceState == STARTED)
    withJdbcStatement() { _ => }
    eventually(Timeout(200.seconds)) {
      assert(engine.getServiceState == STOPPED)
    }
  }
}
