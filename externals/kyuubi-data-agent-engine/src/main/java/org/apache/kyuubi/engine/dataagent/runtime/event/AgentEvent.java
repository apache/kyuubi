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

/**
 * Base class for events emitted by the ReAct agent loop. Each event represents a discrete step in
 * the agent's reasoning and execution process, enabling real-time token-level streaming to clients.
 *
 * <p>Every subclass must declare its {@link EventType}, which also determines the SSE event name
 * used on the wire. This allows consumers to {@code switch} on the type rather than using {@code
 * instanceof} chains.
 *
 * <p>Package-private constructor restricts subclassing to this package.
 */
public abstract class AgentEvent {

  private final EventType eventType;

  AgentEvent(EventType eventType) {
    this.eventType = eventType;
  }

  /** Returns the type of this event. */
  public EventType eventType() {
    return eventType;
  }
}
