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

package org.apache.kyuubi.engine.dataagent.runtime.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.kyuubi.engine.dataagent.tool.ToolRiskLevel;
import org.junit.jupiter.api.Test;

public class EventTest {

  @Test
  public void testContentDeltaLongTextTruncated() {
    String longText = new String(new char[300]).replace('\0', 'a');
    ContentDelta event = new ContentDelta(longText);
    String str = event.toString();
    assertTrue(str.contains("..."));
    assertTrue(str.length() < longText.length() + 50);
  }

  @Test
  public void testContentDeltaNullText() {
    ContentDelta event = new ContentDelta(null);
    assertNull(event.text());
  }

  @Test
  public void testContentCompleteNull() {
    ContentComplete event = new ContentComplete(null);
    assertTrue(event.toString().contains("length=0"));
  }

  @Test
  public void testToolCallArgsImmutable() {
    Map<String, Object> args = new HashMap<>();
    args.put("key", "value");
    ToolCall event = new ToolCall("tc-1", "tool", args);
    assertThrows(UnsupportedOperationException.class, () -> event.toolArgs().put("new", "entry"));
  }

  @Test
  public void testToolCallNullArgs() {
    ToolCall event = new ToolCall("tc-1", "tool", null);
    assertNotNull(event.toolArgs());
    assertTrue(event.toolArgs().isEmpty());
  }

  @Test
  public void testToolResultError() {
    ToolResult event = new ToolResult("tc-2", "sql_query", "syntax error", true);
    assertTrue(event.isError());
    assertTrue(event.toString().contains("isError=true"));
  }

  @Test
  public void testToolResultLongOutputTruncated() {
    String longOutput = new String(new char[300]).replace('\0', 'x');
    ToolResult event = new ToolResult("tc-1", "tool", longOutput, false);
    String str = event.toString();
    assertTrue(str.contains("..."));
  }

  @Test
  public void testApprovalRequestArgsImmutable() {
    Map<String, Object> args = new HashMap<>();
    args.put("key", "value");
    ApprovalRequest event = new ApprovalRequest("req-1", "tc-1", "tool", args, ToolRiskLevel.SAFE);
    assertThrows(UnsupportedOperationException.class, () -> event.toolArgs().put("new", "entry"));
  }

  @Test
  public void testApprovalRequestNullArgs() {
    ApprovalRequest event = new ApprovalRequest("req-1", "tc-1", "tool", null, ToolRiskLevel.SAFE);
    assertNotNull(event.toolArgs());
    assertTrue(event.toolArgs().isEmpty());
  }

  @Test
  public void testEventTypeSseNames() {
    assertEquals("agent_start", EventType.AGENT_START.sseEventName());
    assertEquals("step_start", EventType.STEP_START.sseEventName());
    assertEquals("content_delta", EventType.CONTENT_DELTA.sseEventName());
    assertEquals("reasoning_delta", EventType.REASONING_DELTA.sseEventName());
    assertEquals("content_complete", EventType.CONTENT_COMPLETE.sseEventName());
    assertEquals("tool_call", EventType.TOOL_CALL.sseEventName());
    assertEquals("tool_result", EventType.TOOL_RESULT.sseEventName());
    assertEquals("step_end", EventType.STEP_END.sseEventName());
    assertEquals("error", EventType.ERROR.sseEventName());
    assertEquals("approval_request", EventType.APPROVAL_REQUEST.sseEventName());
    assertEquals("compaction", EventType.COMPACTION.sseEventName());
    assertEquals("agent_finish", EventType.AGENT_FINISH.sseEventName());
  }

  @Test
  public void testAllEventTypesHaveUniqueSseNames() {
    EventType[] values = EventType.values();
    java.util.Set<String> names = new java.util.HashSet<>();
    for (EventType type : values) {
      assertTrue(names.add(type.sseEventName()), "Duplicate SSE name: " + type.sseEventName());
    }
    assertEquals(12, values.length);
  }
}
