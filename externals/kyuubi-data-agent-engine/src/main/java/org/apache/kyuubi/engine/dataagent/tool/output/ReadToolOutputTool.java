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

package org.apache.kyuubi.engine.dataagent.tool.output;

import org.apache.kyuubi.engine.dataagent.runtime.ToolOutputStore;
import org.apache.kyuubi.engine.dataagent.tool.AgentTool;
import org.apache.kyuubi.engine.dataagent.tool.ToolContext;

/**
 * Read a line window from a previously offloaded tool-output file. Companion to {@code
 * ToolResultOffloadMiddleware}.
 */
public class ReadToolOutputTool implements AgentTool<ReadToolOutputArgs> {

  public static final String NAME = "read_tool_output";
  private static final int DEFAULT_LIMIT = 200;
  private static final int MAX_LIMIT = 500;

  private final ToolOutputStore store;

  public ReadToolOutputTool(ToolOutputStore store) {
    this.store = store;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String description() {
    return "Read a line window from a previously offloaded tool-output file "
        + "(the path is supplied in the truncation notice of a prior tool result). "
        + "Returns '[lines X-Y of Z total]' header followed by the requested window. "
        + "Use when a prior tool's output was truncated and you need to inspect more of it.";
  }

  @Override
  public Class<ReadToolOutputArgs> argsType() {
    return ReadToolOutputArgs.class;
  }

  @Override
  public String execute(ToolContext ctx, ReadToolOutputArgs args) {
    if (args == null || args.path == null || args.path.isEmpty()) {
      return "Error: 'path' parameter is required.";
    }
    if (ctx == null || ctx.sessionId() == null) {
      return "Error: read_tool_output requires a session context.";
    }
    int offset = args.offset != null ? args.offset : 0;
    int limit = args.limit != null ? args.limit : DEFAULT_LIMIT;
    if (limit > MAX_LIMIT) limit = MAX_LIMIT;
    return store.read(ctx.sessionId(), args.path, offset, limit);
  }
}
