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

public class SessionData {
  private String identifier;
  private String remoteId;
  private String user;
  private String ipAddr;
  private Map<String, String> conf;
  private Map<String, String> optimizedConf;
  private Long createTime;
  private Long duration;
  private Long idleTime;
  private String exception;
  private String sessionType;
  private String kyuubiInstance;
  private String engineId;
  private String engineName;
  private String engineUrl;
  private String sessionName;
  private Integer totalOperations;

  public SessionData() {}

  public SessionData(
      String identifier,
      String remoteId,
      String user,
      String ipAddr,
      Map<String, String> conf,
      Map<String, String> optimizedConf,
      Long createTime,
      Long duration,
      Long idleTime,
      String exception,
      String sessionType,
      String kyuubiInstance,
      String engineId,
      String engineName,
      String engineUrl,
      String sessionName,
      Integer totalOperations) {
    this.identifier = identifier;
    this.remoteId = remoteId;
    this.user = user;
    this.ipAddr = ipAddr;
    this.conf = conf;
    this.optimizedConf = optimizedConf;
    this.createTime = createTime;
    this.duration = duration;
    this.idleTime = idleTime;
    this.exception = exception;
    this.sessionType = sessionType;
    this.kyuubiInstance = kyuubiInstance;
    this.engineId = engineId;
    this.engineName = engineName;
    this.engineUrl = engineUrl;
    this.sessionName = sessionName;
    this.totalOperations = totalOperations;
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

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getIpAddr() {
    return ipAddr;
  }

  public void setIpAddr(String ipAddr) {
    this.ipAddr = ipAddr;
  }

  public Map<String, String> getConf() {
    if (null == conf) {
      return Collections.emptyMap();
    }
    return conf;
  }

  public void setConf(Map<String, String> conf) {
    this.conf = conf;
  }

  public Map<String, String> getOptimizedConf() {
    return optimizedConf;
  }

  public void setOptimizedConf(Map<String, String> optimizedConf) {
    this.optimizedConf = optimizedConf;
  }

  public Long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(Long createTime) {
    this.createTime = createTime;
  }

  public Long getDuration() {
    return duration;
  }

  public void setDuration(Long duration) {
    this.duration = duration;
  }

  public Long getIdleTime() {
    return idleTime;
  }

  public void setIdleTime(Long idleTime) {
    this.idleTime = idleTime;
  }

  public String getException() {
    return exception;
  }

  public void setException(String exception) {
    this.exception = exception;
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

  public String getEngineId() {
    return engineId;
  }

  public void setEngineId(String engineId) {
    this.engineId = engineId;
  }

  public String getEngineName() {
    return engineName;
  }

  public void setEngineName(String engineName) {
    this.engineName = engineName;
  }

  public String getEngineUrl() {
    return engineUrl;
  }

  public void setEngineUrl(String engineUrl) {
    this.engineUrl = engineUrl;
  }

  public String getSessionName() {
    return sessionName;
  }

  public void setSessionName(String sessionName) {
    this.sessionName = sessionName;
  }

  public Integer getTotalOperations() {
    return totalOperations;
  }

  public void setTotalOperations(Integer totalOperations) {
    this.totalOperations = totalOperations;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SessionData that = (SessionData) o;
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
