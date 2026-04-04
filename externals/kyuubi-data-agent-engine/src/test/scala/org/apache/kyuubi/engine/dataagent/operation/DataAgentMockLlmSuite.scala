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

import java.sql.DriverManager

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.dataagent.WithDataAgentEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper

/**
 * Tests the full tool-call pipeline using MockLlmProvider against a real SQLite database.
 * Validates: provider -> tool call -> SQL execution -> result streaming, without a real LLM.
 */
class DataAgentMockLlmSuite extends HiveJDBCTestHelper with WithDataAgentEngine {

  private val JSON = new ObjectMapper()

  private val dbPath =
    s"${System.getProperty("java.io.tmpdir")}" +
      s"/dataagent_mock_test_${java.util.UUID.randomUUID()}.db"

  override def withKyuubiConf: Map[String, String] = Map(
    ENGINE_DATA_AGENT_PROVIDER.key ->
      "org.apache.kyuubi.engine.dataagent.provider.mock.MockLlmProvider",
    ENGINE_DATA_AGENT_APPROVAL_MODE.key -> "AUTO_APPROVE",
    ENGINE_DATA_AGENT_JDBC_URL.key -> s"jdbc:sqlite:$dbPath")

  override protected def jdbcUrl: String = jdbcConnectionUrl

  override def beforeAll(): Unit = {
    setupTestDatabase()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    cleanupTestDatabase()
  }

  private def setupTestDatabase(): Unit = {
    Class.forName("org.sqlite.JDBC")
    val conn = DriverManager.getConnection(s"jdbc:sqlite:$dbPath")
    try {
      val stmt = conn.createStatement()
      stmt.executeUpdate(
        """CREATE TABLE employees (
          |  id INTEGER PRIMARY KEY,
          |  name TEXT NOT NULL,
          |  department TEXT NOT NULL,
          |  salary REAL
          |)""".stripMargin)
      stmt.executeUpdate(
        "INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 120000)")
      stmt.executeUpdate(
        "INSERT INTO employees VALUES (2, 'Bob', 'Marketing', 95000)")
      stmt.executeUpdate(
        "INSERT INTO employees VALUES (3, 'Charlie', 'Engineering', 110000)")
      stmt.close()
    } finally {
      conn.close()
    }
  }

  private def cleanupTestDatabase(): Unit = {
    new java.io.File(dbPath).delete()
  }

  private def collectReply(rawReply: String): (String, List[JsonNode]) = {
    val textBuilder = new StringBuilder
    val events = new java.util.ArrayList[JsonNode]()
    val parser = JSON.getFactory.createParser(rawReply)
    parser.configure(
      com.fasterxml.jackson.core.JsonParser.Feature.AUTO_CLOSE_SOURCE,
      false)
    try {
      val it = JSON.readValues(parser, classOf[JsonNode])
      while (it.hasNext) {
        val node = it.next()
        events.add(node)
        val eventType = if (node.has("type")) node.get("type").asText() else ""
        if (eventType == "content_delta" && node.has("text")) {
          textBuilder.append(node.get("text").asText())
        }
      }
    } catch {
      case _: Exception =>
    }
    import scala.collection.JavaConverters._
    (textBuilder.toString(), events.asScala.toList)
  }

  private def executeAndCollect(stmt: java.sql.Statement, sql: String): (String, List[JsonNode]) = {
    val result = stmt.executeQuery(sql)
    val sb = new StringBuilder
    while (result.next()) {
      sb.append(result.getString("reply"))
    }
    collectReply(sb.toString())
  }

  test("mock LLM provider executes SQL via tool call") {
    withJdbcStatement() { stmt =>
      val (text, events) = executeAndCollect(
        stmt,
        "List all employee names and departments")
      // Verify tool call event was emitted
      val toolCallEvents = events.filter(_.path("type").asText() == "tool_call")
      assert(toolCallEvents.nonEmpty, "Expected tool_call event")
      assert(toolCallEvents.head.path("name").asText() == "sql_query")

      // Verify tool result event was emitted
      val toolResultEvents = events.filter(_.path("type").asText() == "tool_result")
      assert(toolResultEvents.nonEmpty, "Expected tool_result event")

      // Verify the final answer contains query results
      assert(text.contains("Alice"))
      assert(text.contains("Bob"))
      assert(text.contains("Charlie"))
    }
  }

  test("mock LLM provider handles non-SQL questions without tool calls") {
    withJdbcStatement() { stmt =>
      val (text, events) = executeAndCollect(stmt, "Hello, how are you?")
      // Should not have tool call events
      val toolCallEvents = events.filter(_.path("type").asText() == "tool_call")
      assert(toolCallEvents.isEmpty, "Expected no tool_call events for non-SQL question")

      assert(text.contains("[MockLLM]"))
      assert(text.contains("Hello, how are you?"))
    }
  }

  test("mock LLM provider emits correct event sequence") {
    withJdbcStatement() { stmt =>
      val (_, events) = executeAndCollect(
        stmt,
        "Count the total number of employees")
      val types = events.map(_.path("type").asText())

      // Verify event ordering for a tool-call flow
      // Note: content_complete is internal and not serialized to JSON
      assert(types.contains("agent_start"))
      assert(types.contains("step_start"))
      assert(types.contains("tool_call"))
      assert(types.contains("tool_result"))
      assert(types.contains("content_delta"))
      assert(types.contains("agent_finish"))

      // agent_start should be first, agent_finish should be last
      assert(types.head == "agent_start")
      assert(types.last == "agent_finish")
    }
  }

  test("mock LLM provider returns correct SQL results") {
    withJdbcStatement() { stmt =>
      val (text, _) = executeAndCollect(
        stmt,
        "How many employees in each department")
      // Engineering has 2, Marketing has 1
      assert(text.contains("Engineering"))
      assert(text.contains("Marketing"))
      assert(text.contains("2"))
      assert(text.contains("1"))
    }
  }

  test("provider exception surfaces as JDBC error and does not hang") {
    withJdbcStatement() { stmt =>
      val ex = intercept[java.sql.SQLException] {
        stmt.executeQuery("__error__")
      }
      assert(ex.getMessage.contains("MockLlmProvider simulated failure"))
      // Verify the session is still usable after the error
      val (text, _) = executeAndCollect(stmt, "Hello after error")
      assert(text.contains("[MockLLM]"))
    }
  }
}
