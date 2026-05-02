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
 * Regex-search a previously offloaded tool-output file. Companion to {@code
 * ToolResultOffloadMiddleware}.
 */
public class GrepToolOutputTool implements AgentTool<GrepToolOutputArgs> {

  public static final String NAME = "grep_tool_output";
  private static final int DEFAULT_MAX_MATCHES = 50;

  private final ToolOutputStore store;

  public GrepToolOutputTool(ToolOutputStore store) {
    this.store = store;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String description() {
    return "Regex-search a previously offloaded tool-output file "
        + "(the path is supplied in the truncation notice of a prior tool result). "
        + "Cheaper than read_tool_output when you know what you're looking for. "
        + "Returns matching lines as '<lineNo>:<content>'.";
  }

  @Override
  public Class<GrepToolOutputArgs> argsType() {
    return GrepToolOutputArgs.class;
  }

  @Override
  public String execute(ToolContext ctx, GrepToolOutputArgs args) {
    if (args == null || args.path == null || args.path.isEmpty()) {
      return "Error: 'path' parameter is required.";
    }
    if (ctx == null || ctx.sessionId() == null) {
      return "Error: grep_tool_output requires a session context.";
    }
    int max = args.maxMatches != null ? args.maxMatches : DEFAULT_MAX_MATCHES;
    return store.grep(ctx.sessionId(), args.path, args.pattern, max);
  }
}
