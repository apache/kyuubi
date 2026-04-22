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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

/** Args for {@link ReadToolOutputTool}. */
public class ReadToolOutputArgs {

  @JsonProperty(required = true)
  @JsonPropertyDescription(
      "Absolute path to the offloaded tool-output file, as reported by the truncation notice.")
  public String path;

  @JsonPropertyDescription("0-based line offset into the file. Defaults to 0.")
  public Integer offset;

  @JsonPropertyDescription(
      "Number of lines to return starting at 'offset'. Defaults to 200; capped at 500.")
  public Integer limit;
}
