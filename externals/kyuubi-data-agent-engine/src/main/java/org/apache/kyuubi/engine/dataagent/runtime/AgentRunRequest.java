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

package org.apache.kyuubi.engine.dataagent.runtime;

import java.util.Objects;

/**
 * User-facing request parameters for a single agent invocation. Only contains fields that come from
 * the caller (question, model override, etc.). Framework-level concerns like memory and event
 * consumer are separate method parameters.
 *
 * <p>Adding new per-request options (e.g. temperature, maxTokens) does not require changing the
 * {@code ReactAgent.run()} signature.
 */
public class AgentRunRequest {

  private final String userInput;
  private String modelName;
  private ApprovalMode approvalMode = ApprovalMode.NORMAL;

  public AgentRunRequest(String userInput) {
    this.userInput = Objects.requireNonNull(userInput, "userInput must not be null");
  }

  public String getUserInput() {
    return userInput;
  }

  public String getModelName() {
    return modelName;
  }

  public AgentRunRequest modelName(String modelName) {
    this.modelName = modelName;
    return this;
  }

  public ApprovalMode getApprovalMode() {
    return approvalMode;
  }

  public AgentRunRequest approvalMode(ApprovalMode approvalMode) {
    this.approvalMode = approvalMode;
    return this;
  }
}
