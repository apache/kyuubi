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

package org.apache.kyuubi.client.api.v1.dto;

import java.util.Objects;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class ChatRequest {
  private String text;
  private String model;
  private String approvalMode;

  public ChatRequest() {}

  public ChatRequest(String text) {
    this.text = text;
  }

  public ChatRequest(String text, String model) {
    this.text = text;
    this.model = model;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public String getModel() {
    return model;
  }

  public void setModel(String model) {
    this.model = model;
  }

  public String getApprovalMode() {
    return approvalMode;
  }

  public void setApprovalMode(String approvalMode) {
    this.approvalMode = approvalMode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ChatRequest that = (ChatRequest) o;
    return Objects.equals(getText(), that.getText())
        && Objects.equals(getModel(), that.getModel())
        && Objects.equals(getApprovalMode(), that.getApprovalMode());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getText(), getModel(), getApprovalMode());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
