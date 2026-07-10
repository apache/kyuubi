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

import java.sql.SQLException
import java.util.Locale

import org.apache.kyuubi.{Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.plugin.{StatementInterceptContext, StatementInterceptor, StatementInterceptResult}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TExecuteStatementReq, TFetchOrientation, TFetchResultsReq, TStatusCode}

/**
 * End-to-end coverage of statement interceptors over the real JDBC -> server -> engine path.
 * Requires a packaged engine, so it runs in the engine-backed CI jobs rather than a bare unit run.
 */
class StatementInterceptorSuite extends WithKyuubiServer with HiveJDBCTestHelper {

  override protected def jdbcUrl: String =
    s"jdbc:kyuubi://${server.frontendServices.head.connectionUrl}/;"

  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(KyuubiConf.ENGINE_SHARE_LEVEL, "connection")
      .set(KyuubiConf.ENGINE_SPARK_MAX_INITIAL_WAIT.key, "0")
      // allow the test user to impersonate, so the effective user can differ from the real user
      .set(s"hadoop.proxyuser.${Utils.currentUser}.groups", "*")
      .set(s"hadoop.proxyuser.${Utils.currentUser}.hosts", "*")
      .set(
        KyuubiConf.STATEMENT_INTERCEPTORS,
        Seq(classOf[TestStatementInterceptor].getName))
  }

  test("PROCEED - statement executes unchanged") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT 1 AS id")
      assert(rs.next())
      assert(rs.getInt("id") === 1)
      assert(!rs.next())
    }
  }

  test("REWRITE - the rewritten statement is executed") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT 'rewrite_me' AS result")
      assert(rs.next())
      assert(rs.getString("result") === "rewritten")
      assert(!rs.next())
    }
  }

  test("REJECT - an engine-routed statement returns an error to the client") {
    withJdbcStatement() { statement =>
      val e = intercept[SQLException] {
        statement.executeQuery("SELECT 'forbidden' AS col")
      }
      assert(e.getMessage.contains("blocked: forbidden keyword"))
    }
  }

  test("REJECT - a server-side command (executeOnServer path) is also intercepted") {
    withJdbcStatement() { statement =>
      val e = intercept[SQLException] {
        statement.execute("KYUUBI DESC SESSION")
      }
      assert(e.getMessage.contains("blocked: server-side command"))
    }
  }

  test("the statement id is exposed to the interceptor") {
    withSessionHandle { (client, sessionHandle) =>
      val request = new TExecuteStatementReq(sessionHandle, "SELECT 'echo_stmt_id' AS x")
      request.setRunAsync(false)
      val response = client.ExecuteStatement(request)
      assert(response.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)

      val operationHandle = response.getOperationHandle
      val fetchRequest = new TFetchResultsReq(
        operationHandle,
        TFetchOrientation.FETCH_NEXT,
        1)
      val fetchResponse = client.FetchResults(fetchRequest)
      assert(fetchResponse.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      val statementId = fetchResponse.getResults.getColumns.get(0).getStringVal.getValues.get(0)
      assert(statementId === OperationHandle(operationHandle).identifier.toString)
    }
  }

  test("the interceptor sees the effective user and the real user under impersonation") {
    val proxyUser = "proxy_alice"
    assert(proxyUser !== Utils.currentUser)
    withSessionConf(Map("hive.server2.proxy.user" -> proxyUser))(Map.empty)(Map.empty) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery("SELECT 'echo_users' AS x")
        assert(rs.next())
        // The interceptor rewrote the query to echo context.user() and context.realUser().
        assert(rs.getString("effective_user") === proxyUser)
        assert(rs.getString("real_user") === Utils.currentUser)
        assert(rs.getString("effective_user") !== rs.getString("real_user"))
        assert(!rs.next())
      }
    }
  }
}

class TestStatementInterceptor extends StatementInterceptor {
  override def beforeExecuteStatement(
      context: StatementInterceptContext): StatementInterceptResult = {
    val sql = context.statement().trim.toLowerCase(Locale.ROOT)
    if (sql.startsWith("kyuubi ")) {
      StatementInterceptResult.reject("blocked: server-side command")
    } else if (sql.contains("forbidden")) {
      StatementInterceptResult.reject("blocked: forbidden keyword")
    } else if (sql.contains("rewrite_me")) {
      StatementInterceptResult.rewrite("SELECT 'rewritten' AS result")
    } else if (sql.contains("echo_stmt_id")) {
      StatementInterceptResult.rewrite(s"SELECT '${context.statementId()}' AS sid")
    } else if (sql.contains("echo_users")) {
      StatementInterceptResult.rewrite(
        s"SELECT '${context.user()}' AS effective_user, '${context.realUser()}' AS real_user")
    } else {
      StatementInterceptResult.proceed()
    }
  }
}
