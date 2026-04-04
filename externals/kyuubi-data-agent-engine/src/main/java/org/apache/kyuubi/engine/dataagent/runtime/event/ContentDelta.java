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

/** A single token or chunk from the LLM streaming response. */
public final class ContentDelta extends AgentEvent {
  private final String text;

  public ContentDelta(String text) {
    super(EventType.CONTENT_DELTA);
    this.text = text;
  }

  public String text() {
    return text;
  }

  @Override
  public String toString() {
    String preview = text != null && text.length() > 200 ? text.substring(0, 200) + "..." : text;
    return "ContentDelta{text='" + preview + "'}";
  }
}
