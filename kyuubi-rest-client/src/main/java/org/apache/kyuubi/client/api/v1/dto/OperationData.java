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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class OperationData {
  private String identifier;
  private String remoteId;
  private String statement;
  private String state;
  private Long createTime;
  private Long startTime;
  private Long completeTime;
  private String exception;
  private String sessionId;
  private String sessionUser;
  private String sessionType;
  private String kyuubiInstance;
  private Map<String, String> metrics;
  private OperationProgress progress;

  public OperationData() {}

  public OperationData(
      String identifier,
      String remoteId,
      String statement,
      String state,
      Long createTime,
      Long startTime,
      Long completeTime,
      String exception,
      String sessionId,
      String sessionUser,
      String sessionType,
      String kyuubiInstance,
      Map<String, String> metrics,
      OperationProgress progress) {
    this.identifier = identifier;
    this.remoteId = remoteId;
    this.statement = statement;
    this.state = state;
    this.createTime = createTime;
    this.startTime = startTime;
    this.completeTime = completeTime;
    this.exception = exception;
    this.sessionId = sessionId;
    this.sessionUser = sessionUser;
    this.sessionType = sessionType;
    this.kyuubiInstance = kyuubiInstance;
    this.metrics = metrics;
    this.progress = progress;
  }

  public String getIdentifier() {
    return identifier;
  }

  public void setIdentifier(String identifier) {
    this.identifier = identifier;
  }

  public String getRemoteId() {
    return remoteId;
  }

  public void setRemoteId(String remoteId) {
    this.remoteId = remoteId;
  }

  public String getStatement() {
    return statement;
  }

  public void setStatement(String statement) {
    this.statement = statement;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public Long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(Long createTime) {
    this.createTime = createTime;
  }

  public Long getStartTime() {
    return startTime;
  }

  public void setStartTime(Long startTime) {
    this.startTime = startTime;
  }

  public Long getCompleteTime() {
    return completeTime;
  }

  public void setCompleteTime(Long completeTime) {
    this.completeTime = completeTime;
  }

  public String getException() {
    return exception;
  }

  public void setException(String exception) {
    this.exception = exception;
  }

  public String getSessionId() {
    return sessionId;
  }

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  public String getSessionUser() {
    return sessionUser;
  }

  public void setSessionUser(String sessionUser) {
    this.sessionUser = sessionUser;
  }

  public String getSessionType() {
    return sessionType;
  }

  public void setSessionType(String sessionType) {
    this.sessionType = sessionType;
  }

  public String getKyuubiInstance() {
    return kyuubiInstance;
  }

  public void setKyuubiInstance(String kyuubiInstance) {
    this.kyuubiInstance = kyuubiInstance;
  }

  public Map<String, String> getMetrics() {
    if (null == metrics) {
      return Collections.emptyMap();
    }
    return metrics;
  }

  public void setMetrics(Map<String, String> metrics) {
    this.metrics = metrics;
  }

  public OperationProgress getProgress() {
    return progress;
  }

  public void setProgress(OperationProgress progress) {
    this.progress = progress;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OperationData that = (OperationData) o;
    return Objects.equals(getIdentifier(), that.getIdentifier());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getIdentifier());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
