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

package org.apache.kyuubi.engine.dataagent.provider.echo;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kyuubi.engine.dataagent.provider.ProviderRunRequest;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentEvent;
import org.apache.kyuubi.engine.dataagent.runtime.event.ContentComplete;
import org.apache.kyuubi.engine.dataagent.runtime.event.ContentDelta;
import org.apache.kyuubi.engine.dataagent.runtime.event.EventType;
import org.junit.Test;

/** Smoke tests for EchoProvider — verify event stream structure and content echo. */
public class EchoProviderTest {

  @Test
  public void testEventSequenceAndContentEcho() {
    EchoProvider provider = new EchoProvider();
    List<AgentEvent> events = new ArrayList<>();

    provider.open("session-1", "testuser");
    provider.run("session-1", new ProviderRunRequest("What is Kyuubi?"), events::add);
    provider.close("session-1");

    assertFalse("Should emit events", events.isEmpty());

    // Verify event type sequence: AGENT_START -> STEP_START -> CONTENT_DELTA... ->
    //   CONTENT_COMPLETE -> STEP_END -> AGENT_FINISH
    List<EventType> types = events.stream().map(AgentEvent::eventType).collect(Collectors.toList());
    assertEquals(EventType.AGENT_START, types.get(0));
    assertEquals(EventType.STEP_START, types.get(1));
    assertEquals(EventType.AGENT_FINISH, types.get(types.size() - 1));
    assertTrue(types.contains(EventType.CONTENT_DELTA));
    assertTrue(types.contains(EventType.CONTENT_COMPLETE));
    assertTrue(types.contains(EventType.STEP_END));

    // Verify deltas concatenate to the complete content, which echoes the question
    StringBuilder deltas = new StringBuilder();
    String complete = null;
    for (AgentEvent event : events) {
      if (event instanceof ContentDelta) {
        deltas.append(((ContentDelta) event).text());
      }
      if (event instanceof ContentComplete) {
        complete = ((ContentComplete) event).fullText();
      }
    }
    assertNotNull(complete);
    assertEquals(complete, deltas.toString());
    assertTrue("Should echo the question", complete.contains("What is Kyuubi?"));
  }
}
