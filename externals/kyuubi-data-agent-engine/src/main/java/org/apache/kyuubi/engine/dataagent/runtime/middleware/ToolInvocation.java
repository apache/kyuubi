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

import java.util.Collections;
import java.util.Map;

/** A single tool invocation: id, name, parsed arguments. Immutable. */
public final class ToolInvocation {
  private final String id;
  private final String name;
  private final Map<String, Object> args;

  public ToolInvocation(String id, String name, Map<String, Object> args) {
    this.id = id;
    this.name = name;
    this.args = Collections.unmodifiableMap(args);
  }

  public String id() {
    return id;
  }

  public String name() {
    return name;
  }

  public Map<String, Object> args() {
    return args;
  }

  public ToolInvocation withArgs(Map<String, Object> newArgs) {
    return new ToolInvocation(id, name, newArgs);
  }
}
