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

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.dataagent.WithDataAgentEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper

/**
 * End-to-end test for the Data Agent engine.
 * Full pipeline: JDBC Client -> Kyuubi Thrift -> DataAgentEngine
 * -> LLM -> Tools -> SQLite -> Results
 *
 * Requires DATA_AGENT_LLM_API_KEY and DATA_AGENT_LLM_API_URL environment variables.
 */
class DataAgentE2ESuite extends HiveJDBCTestHelper with WithDataAgentEngine {

  private val apiKey = sys.env.getOrElse("DATA_AGENT_LLM_API_KEY", "")
  private val apiUrl = sys.env.getOrElse("DATA_AGENT_LLM_API_URL", "")
  private val modelName = sys.env.getOrElse("DATA_AGENT_LLM_MODEL", "")
  private val dbPath =
    s"${System.getProperty("java.io.tmpdir")}/dataagent_e2e_test_${java.util.UUID.randomUUID()}.db"

  override def withKyuubiConf: Map[String, String] = Map(
    ENGINE_DATA_AGENT_PROVIDER.key -> "OPENAI_COMPATIBLE",
    ENGINE_DATA_AGENT_LLM_API_KEY.key -> apiKey,
    ENGINE_DATA_AGENT_LLM_API_URL.key -> apiUrl,
    ENGINE_DATA_AGENT_LLM_MODEL.key -> modelName,
    ENGINE_DATA_AGENT_MAX_ITERATIONS.key -> "10",
    ENGINE_DATA_AGENT_APPROVAL_MODE.key -> "AUTO_APPROVE",
    ENGINE_DATA_AGENT_JDBC_URL.key -> s"jdbc:sqlite:$dbPath")

  override protected def jdbcUrl: String = jdbcConnectionUrl

  private val enabled: Boolean = apiKey.nonEmpty && apiUrl.nonEmpty

  override def beforeAll(): Unit = {
    if (enabled) {
      setupTestDatabase()
      super.beforeAll()
    }
  }

  override def afterAll(): Unit = {
    if (enabled) {
      super.afterAll()
      cleanupTestDatabase()
    }
  }

  private def setupTestDatabase(): Unit = {
    // Clean up any previous test DB
    new java.io.File(dbPath).delete()
    val conn = DriverManager.getConnection(s"jdbc:sqlite:$dbPath")
    try {
      val stmt = conn.createStatement()
      // Create departments table
      stmt.execute("""
        CREATE TABLE departments (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          location TEXT
        )""")
      stmt.execute("INSERT INTO departments VALUES (1, 'Engineering', 'Beijing')")
      stmt.execute("INSERT INTO departments VALUES (2, 'Sales', 'Shanghai')")
      stmt.execute("INSERT INTO departments VALUES (3, 'Marketing', 'Hangzhou')")

      // Create employees table
      stmt.execute("""
        CREATE TABLE employees (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          department_id INTEGER,
          salary REAL,
          hire_date TEXT,
          FOREIGN KEY (department_id) REFERENCES departments(id)
        )""")
      stmt.execute("INSERT INTO employees VALUES (1, 'Alice', 1, 25000, '2022-01-15')")
      stmt.execute("INSERT INTO employees VALUES (2, 'Bob', 1, 30000, '2021-06-01')")
      stmt.execute("INSERT INTO employees VALUES (3, 'Charlie', 2, 20000, '2023-03-10')")
      stmt.execute("INSERT INTO employees VALUES (4, 'Diana', 2, 22000, '2022-09-20')")
      stmt.execute("INSERT INTO employees VALUES (5, 'Eve', 3, 18000, '2023-07-01')")
      stmt.execute("INSERT INTO employees VALUES (6, 'Frank', 1, 35000, '2020-04-15')")
    } finally {
      conn.close()
    }
  }

  private def cleanupTestDatabase(): Unit = {
    new java.io.File(dbPath).delete()
  }

  private val mapper = new ObjectMapper()

  private def drainReply(rs: java.sql.ResultSet): String = {
    val sb = new StringBuilder
    while (rs.next()) {
      sb.append(rs.getString("reply"))
    }
    val stream = sb.toString()
    info(s"Agent event stream: $stream")
    stream
  }

  /**
   * The JDBC `reply` column is a concatenated stream of SSE events
   * (`agent_start`, `tool_call`, `tool_result`, `content_delta`, ...). Only
   * `content_delta.text` is actual model output - this pulls those out and
   * joins them to recover the final natural-language answer.
   */
  private def extractAnswer(eventStream: String): String = {
    val parser = mapper.getFactory.createParser(eventStream)
    val sb = new StringBuilder
    try {
      mapper.readValues(parser, classOf[JsonNode]).asScala.foreach { node =>
        if ("content_delta" == node.path("type").asText()) {
          sb.append(node.path("text").asText(""))
        }
      }
    } finally {
      parser.close()
    }
    sb.toString()
  }

  private val strictFormatHint =
    "Respond with ONLY the answer, no explanation, no markdown, no punctuation."

  test("E2E: agent answers data question through full Kyuubi pipeline") {
    assume(enabled, "DATA_AGENT_LLM_API_KEY/API_URL not set, skipping E2E tests")
    withJdbcStatement() { stmt =>
      val stream = drainReply(
        stmt.executeQuery(
          s"Which department has the highest average salary? $strictFormatHint"))
      assert(extractAnswer(stream) == "Engineering")
    }
  }

  test("E2E: agent resolves follow-up question using prior conversation context") {
    assume(enabled, "DATA_AGENT_LLM_API_KEY/API_URL not set, skipping E2E tests")
    // Two executeQuery calls on the same Statement share the JDBC session, which means
    // the provider reuses the same ConversationMemory across turns. Turn 2 uses the
    // demonstrative "that department" - it can only be answered correctly if Turn 1's
    // answer (Engineering) is carried over in the agent's conversation history.
    withJdbcStatement() { stmt =>
      val stream1 = drainReply(
        stmt.executeQuery(
          s"Which department has the highest average salary? $strictFormatHint"))
      assert(extractAnswer(stream1) == "Engineering")

      // Engineering has 3 employees (Alice, Bob, Frank). If memory is not shared
      // the agent cannot resolve "that department" and cannot produce the exact
      // integer 3 - nothing in Turn 2's prompt points to Engineering.
      val stream2 = drainReply(
        stmt.executeQuery(
          s"How many employees work in that department? $strictFormatHint"))
      assert(extractAnswer(stream2) == "3")
    }
  }
}
