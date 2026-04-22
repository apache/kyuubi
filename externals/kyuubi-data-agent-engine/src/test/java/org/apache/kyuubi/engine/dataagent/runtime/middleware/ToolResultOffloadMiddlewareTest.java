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

package org.apache.kyuubi.engine.dataagent.runtime.middleware;

import static org.junit.Assert.*;

import java.util.Collections;
import org.apache.kyuubi.engine.dataagent.runtime.AgentRunContext;
import org.apache.kyuubi.engine.dataagent.runtime.ApprovalMode;
import org.apache.kyuubi.engine.dataagent.runtime.ConversationMemory;
import org.apache.kyuubi.engine.dataagent.tool.output.GrepToolOutputTool;
import org.apache.kyuubi.engine.dataagent.tool.output.ReadToolOutputTool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ToolResultOffloadMiddlewareTest {

  private ToolResultOffloadMiddleware mw;
  private AgentRunContext ctxWithSession;
  private AgentRunContext ctxNoSession;

  @Before
  public void setUp() {
    mw = new ToolResultOffloadMiddleware();
    ctxWithSession =
        new AgentRunContext(new ConversationMemory(), ApprovalMode.AUTO_APPROVE, "sess-1");
    ctxNoSession = new AgentRunContext(new ConversationMemory(), ApprovalMode.AUTO_APPROVE, null);
  }

  @After
  public void tearDown() {
    mw.onStop();
  }

  @Test
  public void underThresholdPassesThrough() {
    String small = "row1\nrow2\nrow3\n";
    String out =
        mw.afterToolCall(ctxWithSession, "run_select_query", Collections.emptyMap(), small);
    assertNull(out);
  }

  @Test
  public void overLineThresholdTriggersOffload() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 600; i++) sb.append("row").append(i).append('\n');
    String out =
        mw.afterToolCall(ctxWithSession, "run_select_query", Collections.emptyMap(), sb.toString());

    assertNotNull(out);
    assertTrue(out, out.contains("Tool output truncated"));
    assertTrue(out, out.contains("Saved to:"));
    assertTrue(out, out.contains(ReadToolOutputTool.NAME));
    assertTrue(out, out.contains(GrepToolOutputTool.NAME));
    assertTrue(out, out.contains("row0"));
    assertTrue(out, out.contains("row599"));
  }

  @Test
  public void overByteThresholdTriggersOffload() {
    // 60 lines of ~1 KB each = ~60 KB — over the byte threshold but well under the line threshold.
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 60; i++) {
      for (int j = 0; j < 1024; j++) sb.append('a');
      sb.append('\n');
    }
    String out =
        mw.afterToolCall(ctxWithSession, "run_select_query", Collections.emptyMap(), sb.toString());

    assertNotNull("byte threshold should trigger", out);
    assertTrue(out, out.contains("Tool output truncated"));
  }

  @Test
  public void retrievalToolsAreExemptFromGate() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 2000; i++) sb.append("row").append(i).append('\n');
    assertNull(
        mw.afterToolCall(
            ctxWithSession, ReadToolOutputTool.NAME, Collections.emptyMap(), sb.toString()));
    assertNull(
        mw.afterToolCall(
            ctxWithSession, GrepToolOutputTool.NAME, Collections.emptyMap(), sb.toString()));
  }

  @Test
  public void missingSessionIdPassesThrough() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 1000; i++) sb.append("row").append(i).append('\n');
    String out =
        mw.afterToolCall(ctxNoSession, "run_select_query", Collections.emptyMap(), sb.toString());
    assertNull("without sessionId, cannot offload safely — pass through", out);
  }

  @Test
  public void onSessionCloseClearsCounterAndFiles() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 600; i++) sb.append("row").append(i).append('\n');
    mw.afterToolCall(ctxWithSession, "run_select_query", Collections.emptyMap(), sb.toString());
    assertEquals(1, mw.trackedSessions());

    mw.onSessionClose("sess-1");
    assertEquals(0, mw.trackedSessions());
  }

  @Test
  public void multipleOffloadsReuseSameSessionDir() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 600; i++) sb.append("row").append(i).append('\n');
    String out1 =
        mw.afterToolCall(ctxWithSession, "run_select_query", Collections.emptyMap(), sb.toString());
    String out2 =
        mw.afterToolCall(ctxWithSession, "run_select_query", Collections.emptyMap(), sb.toString());
    // Both previews reference the same session dir, different file names.
    assertNotEquals(extractPath(out1), extractPath(out2));
    assertTrue(extractPath(out1).contains("sess-1"));
    assertTrue(extractPath(out2).contains("sess-1"));
  }

  private static String extractPath(String preview) {
    int i = preview.indexOf("Saved to:");
    int eol = preview.indexOf('\n', i);
    return preview.substring(i + "Saved to:".length(), eol).trim();
  }
}
