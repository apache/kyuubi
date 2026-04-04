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
  private val modelName = sys.env.getOrElse("DATA_AGENT_LLM_MODEL", "gpt-4o")
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

  test("E2E: agent answers data question through full Kyuubi pipeline") {
    assume(enabled, "DATA_AGENT_LLM_API_KEY/API_URL not set, skipping E2E tests")
    // scalastyle:off println
    withJdbcStatement() { stmt =>
      // Ask a question that requires schema exploration + SQL execution
      val result = stmt.executeQuery(
        "Which department has the highest average salary?")

      val sb = new StringBuilder
      while (result.next()) {
        val chunk = result.getString("reply")
        sb.append(chunk)
        print(chunk) // real-time output for debugging
      }
      println()

      val reply = sb.toString()

      // The agent should have:
      // 1. Explored the schema (mentioned table names or columns)
      // 2. Executed SQL (the reply should contain actual data)
      // 3. Answered with "Engineering" (avg salary 30000)
      assert(reply.nonEmpty, "Agent should return a non-empty response")
      assert(
        reply.toLowerCase.contains("engineering") || reply.contains("30000"),
        s"Expected the answer to mention 'Engineering' or '30000', got: ${reply.take(500)}")
    }
    // scalastyle:on println
  }
}
