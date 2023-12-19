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
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class OperationProgress {
  private List<String> headerNames;
  private List<List<String>> rows;
  private double progressedPercentage;
  private String status;
  private String footerSummary;
  private long startTime;

  public OperationProgress() {}

  public OperationProgress(
      List<String> headerNames,
      List<List<String>> rows,
      double progressedPercentage,
      String status,
      String footerSummary,
      long startTime) {
    this.headerNames = headerNames;
    this.rows = rows;
    this.progressedPercentage = progressedPercentage;
    this.status = status;
    this.footerSummary = footerSummary;
    this.startTime = startTime;
  }

  public List<String> getHeaderNames() {
    if (headerNames == null) {
      return Collections.emptyList();
    }
    return headerNames;
  }

  public void setHeaderNames(List<String> headerNames) {
    this.headerNames = headerNames;
  }

  public List<List<String>> getRows() {
    if (rows == null) {
      return Collections.emptyList();
    }
    return rows;
  }

  public void setRows(List<List<String>> rows) {
    this.rows = rows;
  }

  public double getProgressedPercentage() {
    return progressedPercentage;
  }

  public void setProgressedPercentage(double progressedPercentage) {
    this.progressedPercentage = progressedPercentage;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getFooterSummary() {
    return footerSummary;
  }

  public void setFooterSummary(String footerSummary) {
    this.footerSummary = footerSummary;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OperationProgress that = (OperationProgress) o;
    return Double.compare(getProgressedPercentage(), that.getProgressedPercentage()) == 0
        && getStartTime() == that.getStartTime()
        && Objects.equals(getHeaderNames(), that.getHeaderNames())
        && Objects.equals(getRows(), that.getRows())
        && Objects.equals(getStatus(), that.getStatus())
        && Objects.equals(getFooterSummary(), that.getFooterSummary());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getHeaderNames(),
        getRows(),
        getProgressedPercentage(),
        getStatus(),
        getFooterSummary(),
        getStartTime());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
