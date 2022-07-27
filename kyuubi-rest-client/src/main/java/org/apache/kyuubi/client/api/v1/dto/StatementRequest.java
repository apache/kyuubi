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

public class StatementRequest {
  private String statement;
  private boolean runAsync;
  private Long queryTimeout;

  public StatementRequest() {}

  public StatementRequest(String statement, boolean runAsync, Long queryTimeout) {
    this.statement = statement;
    this.runAsync = runAsync;
    this.queryTimeout = queryTimeout;
  }

  public String getStatement() {
    return statement;
  }

  public void setStatement(String statement) {
    this.statement = statement;
  }

  public boolean isRunAsync() {
    return runAsync;
  }

  public void setRunAsync(boolean runAsync) {
    this.runAsync = runAsync;
  }

  public Long getQueryTimeout() {
    return queryTimeout;
  }

  public void setQueryTimeout(Long queryTimeout) {
    this.queryTimeout = queryTimeout;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StatementRequest that = (StatementRequest) o;
    return isRunAsync() == that.isRunAsync()
        && Objects.equals(getStatement(), that.getStatement())
        && Objects.equals(getQueryTimeout(), that.getQueryTimeout());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getStatement(), isRunAsync(), getQueryTimeout());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
