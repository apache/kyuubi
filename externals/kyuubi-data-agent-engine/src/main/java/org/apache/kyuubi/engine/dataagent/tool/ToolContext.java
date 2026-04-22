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

package org.apache.kyuubi.engine.dataagent.tool;

/**
 * Per-invocation context handed to {@link AgentTool#execute(Object, ToolContext)}. Today it carries
 * just the session id so session-scoped tools (e.g. the offloaded tool-output retrievers) can
 * restrict their filesystem view; extend here when a tool needs user/approval/etc.
 */
public final class ToolContext {

  /** Sentinel for call sites that have no session to attribute — tests, direct CLI use. */
  public static final ToolContext EMPTY = new ToolContext(null);

  private final String sessionId;

  public ToolContext(String sessionId) {
    this.sessionId = sessionId;
  }

  /** Upstream session id, or {@code null} when invoked outside a session. */
  public String sessionId() {
    return sessionId;
  }
}
