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

package org.apache.kyuubi.engine.dataagent.runtime;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentEvent;
import org.apache.kyuubi.engine.dataagent.runtime.event.ContentDelta;
import org.junit.Test;

public class AgentContextTest {

  @Test
  public void testTokenUsageAccumulates() {
    AgentContext ctx = new AgentContext(new ConversationMemory(), ApprovalMode.AUTO_APPROVE);
    ctx.addTokenUsage(10, 20, 30);
    ctx.addTokenUsage(5, 15, 20);
    assertEquals(15, ctx.getPromptTokens());
    assertEquals(35, ctx.getCompletionTokens());
    assertEquals(50, ctx.getTotalTokens());
  }

  @Test
  public void testEmitWithEmitter() {
    AgentContext ctx = new AgentContext(new ConversationMemory(), ApprovalMode.AUTO_APPROVE);
    List<AgentEvent> events = new ArrayList<>();
    ctx.setEventEmitter(events::add);

    ctx.emit(new ContentDelta("hello"));
    assertEquals(1, events.size());
  }

  @Test
  public void testEmitWithoutEmitterDoesNotThrow() {
    AgentContext ctx = new AgentContext(new ConversationMemory(), ApprovalMode.AUTO_APPROVE);
    // No emitter set — should not throw
    ctx.emit(new ContentDelta("hello"));
  }
}
