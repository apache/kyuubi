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

public class SQLDetail {
  private String sessionId;
  private String operationId;
  private String sessionUser;
  private String statementId;
  private Long createTime;
  private Long completeTime;
  private String statement;
  private String engineName;
  private String engineType;
  private String engineShareLevel;
  private String exception;

  public SQLDetail(
      String sessionId,
      String operationId,
      String sessionUser,
      String statementId,
      Long createTime,
      Long completeTime,
      String statement,
      String engineName,
      String engineType,
      String engineShareLevel,
      String exception) {
    this.sessionId = sessionId;
    this.operationId = operationId;
    this.sessionUser = sessionUser;
    this.statementId = statementId;
    this.createTime = createTime;
    this.completeTime = completeTime;
    this.statement = statement;
    this.engineName = engineName;
    this.engineType = engineType;
    this.engineShareLevel = engineShareLevel;
    this.exception = exception;
  }

  public String getSessionId() {
    return sessionId;
  }

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  public String getOperationId() {
    return operationId;
  }

  public void setOperationId(String operationId) {
    this.operationId = operationId;
  }

  public String getSessionUser() {
    return sessionUser;
  }

  public void setSessionUser(String sessionUser) {
    this.sessionUser = sessionUser;
  }

  public String getStatementId() {
    return statementId;
  }

  public void setStatementId(String statementId) {
    this.statementId = statementId;
  }

  public Long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(Long createTime) {
    this.createTime = createTime;
  }

  public Long getCompleteTime() {
    return completeTime;
  }

  public void setCompleteTime(Long completeTime) {
    this.completeTime = completeTime;
  }

  public String getStatement() {
    return statement;
  }

  public void setStatement(String statement) {
    this.statement = statement;
  }

  public String getEngineName() {
    return engineName;
  }

  public void setEngineName(String engineName) {
    this.engineName = engineName;
  }

  public String getEngineType() {
    return engineType;
  }

  public void setEngineType(String engineType) {
    this.engineType = engineType;
  }

  public String getEngineShareLevel() {
    return engineShareLevel;
  }

  public void setEngineShareLevel(String engineShareLevel) {
    this.engineShareLevel = engineShareLevel;
  }

  public String getException() {
    return exception;
  }

  public void setException(String exception) {
    this.exception = exception;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SQLDetail that = (SQLDetail) o;
    return Objects.equals(sessionId, that.sessionId)
        && Objects.equals(operationId, that.operationId)
        && Objects.equals(sessionUser, that.sessionUser)
        && Objects.equals(statementId, that.statementId)
        && Objects.equals(createTime, that.createTime)
        && Objects.equals(completeTime, that.completeTime)
        && Objects.equals(statement, that.statement)
        && Objects.equals(engineName, that.engineName)
        && Objects.equals(engineType, that.engineType)
        && Objects.equals(engineShareLevel, that.engineShareLevel)
        && Objects.equals(exception, that.exception);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        sessionId,
        operationId,
        sessionUser,
        statementId,
        createTime,
        completeTime,
        statement,
        engineName,
        engineType,
        engineShareLevel,
        exception);
  }
}
