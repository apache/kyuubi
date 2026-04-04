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

/**
 * User-facing request parameters for a provider-level agent invocation. Only contains fields from
 * the caller (question, model override, etc.). Adding new per-request options does not require
 * changing the {@link DataAgentProvider} interface.
 */
public class ProviderRunRequest {

  private final String question;
  private String modelName;
  private String approvalMode;

  public ProviderRunRequest(String question) {
    this.question = question;
  }

  public String getQuestion() {
    return question;
  }

  public String getModelName() {
    return modelName;
  }

  public ProviderRunRequest modelName(String modelName) {
    this.modelName = modelName;
    return this;
  }

  public String getApprovalMode() {
    return approvalMode;
  }

  public ProviderRunRequest approvalMode(String approvalMode) {
    this.approvalMode = approvalMode;
    return this;
  }
}
