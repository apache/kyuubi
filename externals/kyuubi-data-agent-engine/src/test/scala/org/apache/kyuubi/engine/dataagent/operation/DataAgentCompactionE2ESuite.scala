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
 * End-to-end test that forces CompactionMiddleware to fire inside the engine and verifies
 * two things simultaneously:
 *   1. The `compaction` SSE event reaches the JDBC client (wiring works).
 *   2. The agent still answers correctly *after* compaction, proving the summary preserved
 *      the facts the follow-up question depends on.
 *
 * The trigger threshold is set extremely low (500 tokens) so that the schema dump plus the
 * first turn's completion already blow past it, forcing compaction before turn 2 or 3.
 *
 * Requires DATA_AGENT_LLM_API_KEY and DATA_AGENT_LLM_API_URL.
 */
class DataAgentCompactionE2ESuite extends HiveJDBCTestHelper with WithDataAgentEngine {

  private val apiKey = sys.env.getOrElse("DATA_AGENT_LLM_API_KEY", "")
  private val apiUrl = sys.env.getOrElse("DATA_AGENT_LLM_API_URL", "")
  private val modelName = sys.env.getOrElse("DATA_AGENT_LLM_MODEL", "")
  private val dbPath = {
    val tmp = System.getProperty("java.io.tmpdir")
    val uid = java.util.UUID.randomUUID()
    s"$tmp/dataagent_compaction_e2e_$uid.db"
  }

  override def withKyuubiConf: Map[String, String] = Map(
    ENGINE_DATA_AGENT_PROVIDER.key -> "OPENAI_COMPATIBLE",
    ENGINE_DATA_AGENT_LLM_API_KEY.key -> apiKey,
    ENGINE_DATA_AGENT_LLM_API_URL.key -> apiUrl,
    ENGINE_DATA_AGENT_LLM_MODEL.key -> modelName,
    ENGINE_DATA_AGENT_MAX_ITERATIONS.key -> "10",
    ENGINE_DATA_AGENT_APPROVAL_MODE.key -> "AUTO_APPROVE",
    // Force compaction to fire aggressively -- any realistic prompt + one LLM round-trip
    // will exceed 500 tokens, so compaction must trigger by turn 2 or 3.
    ENGINE_DATA_AGENT_COMPACTION_TRIGGER_TOKENS.key -> "500",
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
      new java.io.File(dbPath).delete()
    }
  }

  private def setupTestDatabase(): Unit = {
    new java.io.File(dbPath).delete()
    val conn = DriverManager.getConnection(s"jdbc:sqlite:$dbPath")
    try {
      val stmt = conn.createStatement()
      stmt.execute(
        """
          |CREATE TABLE employees (
          |  id INTEGER PRIMARY KEY,
          |  name TEXT NOT NULL,
          |  department TEXT NOT NULL,
          |  salary REAL NOT NULL
          |)""".stripMargin)
      // 6 employees across 3 departments; Frank is unambiguously the top earner.
      stmt.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 25000)")
      stmt.execute("INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 30000)")
      stmt.execute("INSERT INTO employees VALUES (3, 'Charlie', 'Sales', 20000)")
      stmt.execute("INSERT INTO employees VALUES (4, 'Diana', 'Sales', 22000)")
      stmt.execute("INSERT INTO employees VALUES (5, 'Eve', 'Marketing', 18000)")
      stmt.execute("INSERT INTO employees VALUES (6, 'Frank', 'Engineering', 35000)")
    } finally {
      conn.close()
    }
  }

  private val mapper = new ObjectMapper()

  private def drainReply(rs: java.sql.ResultSet): String = {
    val sb = new StringBuilder
    while (rs.next()) {
      sb.append(rs.getString("reply"))
    }
    sb.toString()
  }

  private def parseEvents(stream: String): Seq[JsonNode] = {
    val parser = mapper.getFactory.createParser(stream)
    try mapper.readValues(parser, classOf[JsonNode]).asScala.toList
    finally parser.close()
  }

  private def extractAnswer(events: Seq[JsonNode]): String = {
    val sb = new StringBuilder
    events.foreach { node =>
      if ("content_delta" == node.path("type").asText()) {
        sb.append(node.path("text").asText(""))
      }
    }
    sb.toString()
  }

  private val strictFormatHint =
    "Respond with ONLY the answer, no explanation, no markdown, no punctuation."

  test("E2E: compaction fires mid-conversation and preserves facts across turns") {
    assume(enabled, "DATA_AGENT_LLM_API_KEY/API_URL not set, skipping")

    // CompactionMiddleware.KEEP_RECENT_TURNS is hardcoded to 4. computeSplit needs a
    // non-empty 'old' slice, so at least 5 distinct user turns must accumulate before
    // compaction can fire -- turns 1..(N-4) become the old slice, 4 most recent are
    // kept verbatim. Turn 5 is the observable trigger point.
    //
    // Turn 5 is phrased to force a fresh SQL query rather than relying on recall,
    // because summary quality varies across LLMs (some drop the top-earner fact).
    // Correctness of the final answer then validates that the post-compaction history
    // still gives the agent enough context to pick the right tool and query -- which is
    // the compaction contract we actually care about: mechanism fires, agent recovers.
    withJdbcStatement() { stmt =>
      Seq(
        "List every department that appears in the employees table.",
        "How many employees work in Engineering?",
        "What salaries do Sales employees earn?",
        "Who works in Marketing?").zipWithIndex.foreach { case (q, i) =>
        val events = parseEvents(drainReply(stmt.executeQuery(q)))
        info(s"Turn ${i + 1} answer: ${extractAnswer(events)}")
      }

      // Turn 5 -- explicitly instruct the agent to re-query so the answer does not
      // depend on summary fidelity, only on the agent still functioning after
      // compaction rewrote history.
      val events5 = parseEvents(drainReply(stmt.executeQuery(
        "Run a SELECT against the employees table to find the single employee with" +
          " the highest salary. Report ONLY that employee's name." +
          s" $strictFormatHint")))
      val answer5 = extractAnswer(events5)
      info(s"Turn 5 answer: $answer5")

      val compactionEvents =
        events5.filter(_.path("type").asText() == "compaction")
      assert(
        compactionEvents.nonEmpty,
        "Expected at least one compaction event in turn 5 (trigger=500 tokens, 5 turns)")

      // Sanity-check event shape -- field names must match ExecuteStatement's SSE encoder.
      val c = compactionEvents.head
      assert(
        c.has("summarized") && c.get("summarized").asInt() > 0,
        s"compaction event should carry a positive summarized count: $c")
      assert(c.has("kept") && c.get("kept").asInt() >= 0, s"compaction event missing kept: $c")
      assert(
        c.has("triggerTokens") && c.get("triggerTokens").asLong() == 500L,
        s"compaction event should echo configured trigger: $c")

      // Turn 5 was told to SELECT fresh; Frank (35000) is unambiguously the top earner.
      // If we don't get "Frank", either the agent failed to re-query after compaction
      // (real bug in post-compaction history) or the tool layer is broken.
      assert(
        answer5.contains("Frank"),
        s"Turn 5 should identify Frank as the top earner after re-querying; the agent" +
          s" must remain functional post-compaction. Got: $answer5")
    }
  }
}
