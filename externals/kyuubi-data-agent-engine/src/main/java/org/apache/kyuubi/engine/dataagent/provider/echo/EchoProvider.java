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

import java.util.function.Consumer;
import org.apache.kyuubi.engine.dataagent.provider.DataAgentProvider;
import org.apache.kyuubi.engine.dataagent.provider.ProviderRunRequest;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentEvent;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentFinish;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentStart;
import org.apache.kyuubi.engine.dataagent.runtime.event.ContentComplete;
import org.apache.kyuubi.engine.dataagent.runtime.event.ContentDelta;
import org.apache.kyuubi.engine.dataagent.runtime.event.StepEnd;
import org.apache.kyuubi.engine.dataagent.runtime.event.StepStart;

/** A simple echo provider for testing purposes. Simulates the agent event stream. */
public class EchoProvider implements DataAgentProvider {

  @Override
  public void open(String sessionId, String user) {}

  @Override
  public void run(String sessionId, ProviderRunRequest request, Consumer<AgentEvent> onEvent) {
    String question = request.getQuestion();

    onEvent.accept(new AgentStart());
    onEvent.accept(new StepStart(1));

    // Simulate token-level streaming
    String reply =
        "[DataAgent Echo] I received your question: "
            + question
            + "\n"
            + "This is the Data Agent engine in echo mode. "
            + "Please configure an LLM provider (e.g., OPENAI_COMPATIBLE) for actual data analysis.";
    for (String token : reply.split("(?<=\\s)")) {
      onEvent.accept(new ContentDelta(token));
    }

    onEvent.accept(new ContentComplete(reply));
    onEvent.accept(new StepEnd(1));
    onEvent.accept(new AgentFinish(1, 0, 0, 0));
  }

  @Override
  public void close(String sessionId) {}
}
