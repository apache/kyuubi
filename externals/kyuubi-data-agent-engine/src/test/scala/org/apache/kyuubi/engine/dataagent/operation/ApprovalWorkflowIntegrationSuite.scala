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

package org.apache.kyuubi.engine.dataagent.operation

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.dataagent.WithDataAgentEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class ApprovalWorkflowIntegrationSuite extends HiveJDBCTestHelper with WithDataAgentEngine {

  private val JSON = new ObjectMapper()

  override def withKyuubiConf: Map[String, String] = Map(
    ENGINE_DATA_AGENT_PROVIDER.key -> "echo")

  override protected def jdbcUrl: String = jdbcConnectionUrl

  /** Extract the first JSON result row from the JDBC result set. */
  private def getFirstRow(stmt: java.sql.Statement, sql: String): String = {
    val rs = stmt.executeQuery(sql)
    assert(rs.next(), s"Expected at least one row for: $sql")
    rs.getString("reply")
  }

  /** Parse a JSON response and return the node. */
  private def parseJson(raw: String): JsonNode = {
    JSON.readTree(raw)
  }

  test("__approve: returns not_found for non-existent request") {
    withJdbcStatement() { stmt =>
      val raw = getFirstRow(stmt, "__approve:non-existent-id-123")
      val node = parseJson(raw)
      assert(node.get("status").asText() === "not_found")
      assert(node.get("action").asText() === "approved")
      assert(node.get("requestId").asText() === "non-existent-id-123")
    }
  }

  test("__deny: returns not_found for non-existent request") {
    withJdbcStatement() { stmt =>
      val raw = getFirstRow(stmt, "__deny:non-existent-id-456")
      val node = parseJson(raw)
      assert(node.get("status").asText() === "not_found")
      assert(node.get("action").asText() === "denied")
      assert(node.get("requestId").asText() === "non-existent-id-456")
    }
  }

  test("__approve: with empty requestId throws error") {
    withJdbcStatement() { stmt =>
      val ex = intercept[Exception] {
        stmt.executeQuery("__approve:")
      }
      assert(ex.getMessage.toLowerCase.contains("empty") ||
        ex.getMessage.toLowerCase.contains("requestid"))
    }
  }

  test("__deny: with empty requestId throws error") {
    withJdbcStatement() { stmt =>
      val ex = intercept[Exception] {
        stmt.executeQuery("__deny:")
      }
      assert(ex.getMessage.toLowerCase.contains("empty") ||
        ex.getMessage.toLowerCase.contains("requestid"))
    }
  }

  test("non-approval command routes to echo provider") {
    withJdbcStatement() { stmt =>
      // A statement not starting with __approve: or __deny: will be treated
      // as a regular query (not an approval command), so it goes through the echo
      // provider instead. This verifies routing behavior.
      val rs = stmt.executeQuery("hello world")
      val sb = new StringBuilder
      while (rs.next()) {
        sb.append(rs.getString("reply"))
      }
      // The echo provider wraps response in JSON events -- verify it contains the text
      val raw = sb.toString()
      assert(raw.contains("content_delta"), s"Expected content_delta events in: $raw")
      // "hello" and "world" may be in separate content_delta events
      assert(raw.contains("hello") && raw.contains("world"), s"Expected echo of input in: $raw")
    }
  }

  test("isApprovalCommand correctly identifies prefixes") {
    assert(ApproveToolCall.isApprovalCommand("__approve:abc"))
    assert(ApproveToolCall.isApprovalCommand("__deny:abc"))
    assert(ApproveToolCall.isApprovalCommand("  __approve:abc  "))
    assert(ApproveToolCall.isApprovalCommand("  __deny:abc  "))
    assert(!ApproveToolCall.isApprovalCommand("SELECT 1"))
    assert(!ApproveToolCall.isApprovalCommand("__invalid:abc"))
    assert(!ApproveToolCall.isApprovalCommand(""))
  }
}
