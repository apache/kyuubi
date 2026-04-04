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

package org.apache.kyuubi.engine.dataagent.tool.sql;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

/** Parameter schema for {@link SqlQueryTool}. */
public class SqlQueryArgs {

  @JsonProperty(required = true)
  @JsonPropertyDescription(
      "A SQL statement to execute. Supports SELECT, SHOW, DESCRIBE, and other statements.")
  public String sql;

  @JsonPropertyDescription("Maximum number of rows to return (default: 100)")
  public Integer maxRows = 100;
}
