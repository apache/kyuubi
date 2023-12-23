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

import java.util.Map;

public class KyuubiOperationEvent {

  private String statementId;

  private String remoteId;

  private String statement;

  private boolean shouldRunAsync;

  private String state;

  private long eventTime;

  private long createTime;

  private long startTime;

  private long completeTime;

  private Throwable exception;

  private String sessionId;

  private String sessionUser;

  private String sessionType;

  private String kyuubiInstance;

  private Map<String, String> metrics;

  private OperationProgress progress;

  public KyuubiOperationEvent() {}

  public KyuubiOperationEvent(
      String statementId,
      String remoteId,
      String statement,
      boolean shouldRunAsync,
      String state,
      long eventTime,
      long createTime,
      long startTime,
      long completeTime,
      Throwable exception,
      String sessionId,
      String sessionUser,
      String sessionType,
      String kyuubiInstance,
      Map<String, String> metrics,
      OperationProgress progress) {
    this.statementId = statementId;
    this.remoteId = remoteId;
    this.statement = statement;
    this.shouldRunAsync = shouldRunAsync;
    this.state = state;
    this.eventTime = eventTime;
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

  public static KyuubiOperationEvent.KyuubiOperationEventBuilder builder() {
    return new KyuubiOperationEvent.KyuubiOperationEventBuilder();
  }

  public static class KyuubiOperationEventBuilder {
    private String statementId;

    private String remoteId;

    private String statement;

    private boolean shouldRunAsync;

    private String state;

    private long eventTime;

    private long createTime;

    private long startTime;

    private long completeTime;

    private Throwable exception;

    private String sessionId;

    private String sessionUser;

    private String sessionType;

    private String kyuubiInstance;

    private Map<String, String> metrics;

    private OperationProgress progress;

    public KyuubiOperationEventBuilder() {}

    public KyuubiOperationEvent.KyuubiOperationEventBuilder statementId(final String statementId) {
      this.statementId = statementId;
      return this;
    }

    public KyuubiOperationEvent.KyuubiOperationEventBuilder remoteId(final String remoteId) {
      this.remoteId = remoteId;
      return this;
    }

    public KyuubiOperationEvent.KyuubiOperationEventBuilder statement(final String statement) {
      this.statement = statement;
      return this;
    }

    public KyuubiOperationEvent.KyuubiOperationEventBuilder shouldRunAsync(
        final boolean shouldRunAsync) {
      this.shouldRunAsync = shouldRunAsync;
      return this;
    }

    public KyuubiOperationEvent.KyuubiOperationEventBuilder state(final String state) {
      this.state = state;
      return this;
    }

    public KyuubiOperationEvent.KyuubiOperationEventBuilder eventTime(final long eventTime) {
      this.eventTime = eventTime;
      return this;
    }

    public KyuubiOperationEvent.KyuubiOperationEventBuilder createTime(final long createTime) {
      this.createTime = createTime;
      return this;
    }

    public KyuubiOperationEvent.KyuubiOperationEventBuilder startTime(final long startTime) {
      this.startTime = startTime;
      return this;
    }

    public KyuubiOperationEvent.KyuubiOperationEventBuilder completeTime(final long completeTime) {
      this.completeTime = completeTime;
      return this;
    }

    public KyuubiOperationEvent.KyuubiOperationEventBuilder exception(final Throwable exception) {
      this.exception = exception;
      return this;
    }

    public KyuubiOperationEvent.KyuubiOperationEventBuilder sessionId(final String sessionId) {
      this.sessionId = sessionId;
      return this;
    }

    public KyuubiOperationEvent.KyuubiOperationEventBuilder sessionUser(final String sessionUser) {
      this.sessionUser = sessionUser;
      return this;
    }

    public KyuubiOperationEvent.KyuubiOperationEventBuilder sessionType(final String sessionType) {
      this.sessionType = sessionType;
      return this;
    }

    public KyuubiOperationEvent.KyuubiOperationEventBuilder kyuubiInstance(
        final String kyuubiInstance) {
      this.kyuubiInstance = kyuubiInstance;
      return this;
    }

    public KyuubiOperationEvent.KyuubiOperationEventBuilder metrics(
        final Map<String, String> metrics) {
      this.metrics = metrics;
      return this;
    }

    public KyuubiOperationEvent.KyuubiOperationEventBuilder progress(
        final OperationProgress progress) {
      this.progress = progress;
      return this;
    }

    public KyuubiOperationEvent build() {
      return new KyuubiOperationEvent(
          statementId,
          remoteId,
          statement,
          shouldRunAsync,
          state,
          eventTime,
          createTime,
          startTime,
          completeTime,
          exception,
          sessionId,
          sessionUser,
          sessionType,
          kyuubiInstance,
          metrics,
          progress);
    }
  }

  public String getStatementId() {
    return statementId;
  }

  public void setStatementId(String statementId) {
    this.statementId = statementId;
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

  public boolean isShouldRunAsync() {
    return shouldRunAsync;
  }

  public void setShouldRunAsync(boolean shouldRunAsync) {
    this.shouldRunAsync = shouldRunAsync;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public long getEventTime() {
    return eventTime;
  }

  public void setEventTime(long eventTime) {
    this.eventTime = eventTime;
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getCompleteTime() {
    return completeTime;
  }

  public void setCompleteTime(long completeTime) {
    this.completeTime = completeTime;
  }

  public Throwable getException() {
    return exception;
  }

  public void setException(Throwable exception) {
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
}
