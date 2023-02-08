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

public class Batch {
  private String id;
  private String user;
  private String batchType;
  private String name;
  private long appStartTime;
  private String appId;
  private String appUrl;
  private String appState;
  private String appDiagnostic;
  private String kyuubiInstance;
  private String state;
  private long createTime;
  private long endTime;

  public Batch() {}

  public Batch(
      String id,
      String user,
      String batchType,
      String name,
      long appStartTime,
      String appId,
      String appUrl,
      String appState,
      String appDiagnostic,
      String kyuubiInstance,
      String state,
      long createTime,
      long endTime) {
    this.id = id;
    this.user = user;
    this.batchType = batchType;
    this.name = name;
    this.appStartTime = appStartTime;
    this.appId = appId;
    this.appUrl = appUrl;
    this.appState = appState;
    this.appDiagnostic = appDiagnostic;
    this.kyuubiInstance = kyuubiInstance;
    this.state = state;
    this.createTime = createTime;
    this.endTime = endTime;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getBatchType() {
    return batchType;
  }

  public void setBatchType(String batchType) {
    this.batchType = batchType;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public String getAppUrl() {
    return appUrl;
  }

  public void setAppUrl(String appUrl) {
    this.appUrl = appUrl;
  }

  public String getAppState() {
    return appState;
  }

  public void setAppState(String appState) {
    this.appState = appState;
  }

  public String getAppDiagnostic() {
    return appDiagnostic;
  }

  public void setAppDiagnostic(String appDiagnostic) {
    this.appDiagnostic = appDiagnostic;
  }

  public String getKyuubiInstance() {
    return kyuubiInstance;
  }

  public void setKyuubiInstance(String kyuubiInstance) {
    this.kyuubiInstance = kyuubiInstance;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  public long getAppStartTime() {
    return appStartTime;
  }

  public void setAppStartTime(long appStartTime) {
    this.appStartTime = appStartTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Batch batch = (Batch) o;
    return Objects.equals(getId(), batch.getId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
