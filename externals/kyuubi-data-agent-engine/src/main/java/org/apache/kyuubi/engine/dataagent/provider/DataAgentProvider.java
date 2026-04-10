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

package org.apache.kyuubi.engine.dataagent.provider;

import java.util.function.Consumer;
import org.apache.kyuubi.config.KyuubiConf;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentEvent;
import org.apache.kyuubi.util.reflect.DynConstructors;

/**
 * A pluggable provider interface for the Data Agent engine. Implementations wire up the ReactAgent
 * with an LLM model, tools, and middlewares.
 */
public interface DataAgentProvider {

  /** Initialize a session for the given user. */
  void open(String sessionId, String user);

  /**
   * Run the agent for the given request, emitting events via the consumer. Events include
   * token-level ContentDelta for streaming, ToolCall/ToolResult for tool invocations, and
   * AgentFinish when complete.
   *
   * @param sessionId the session identifier (maps to conversation memory)
   * @param request user-facing parameters (question, model override, etc.)
   * @param onEvent event consumer for streaming events
   */
  void run(String sessionId, ProviderRunRequest request, Consumer<AgentEvent> onEvent);

  /**
   * Close and clean up a single session, releasing session-scoped resources such as conversation
   * history and session state. Called when one user session ends.
   */
  void close(String sessionId);

  /**
   * Resolve a pending tool approval request. Called when the client sends an approval or denial
   * response for a tool call that requires human-in-the-loop confirmation.
   *
   * @param requestId the request ID from the ApprovalRequest event
   * @param approved true to approve, false to deny
   * @return true if the request was found and resolved, false if not found (timed out or invalid)
   */
  default boolean resolveApproval(String requestId, boolean approved) {
    return false;
  }

  /**
   * Stop the provider itself, releasing engine-level resources shared across all sessions (e.g.
   * HTTP connection pools, thread pools). Called once when the entire engine shuts down.
   */
  default void stop() {}

  static DataAgentProvider load(KyuubiConf conf) {
    String providerClass = conf.get(KyuubiConf.ENGINE_DATA_AGENT_PROVIDER());
    try {
      return (DataAgentProvider)
          DynConstructors.builder(DataAgentProvider.class)
              .impl(providerClass, KyuubiConf.class)
              .impl(providerClass)
              .buildChecked()
              .newInstanceChecked(conf);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          "Class "
              + providerClass
              + " is not a child of '"
              + DataAgentProvider.class.getName()
              + "'.",
          e);
    } catch (Exception e) {
      throw new IllegalArgumentException("Error while instantiating '" + providerClass + "': ", e);
    }
  }
}
