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

public class KyuubiServerEvent {
  private String serverName;
  private Long startTime;
  private Long eventTime;
  private String state;
  private String serverIp;
  private Map<String, String> serverConf;
  private Map<String, String> serverEnv;
  private Map<String, String> buildInfo;

  public KyuubiServerEvent() {}

  public KyuubiServerEvent(
      String serverName,
      Long startTime,
      Long eventTime,
      String state,
      String serverIp,
      Map<String, String> serverConf,
      Map<String, String> serverEnv,
      Map<String, String> buildInfo) {
    this.serverName = serverName;
    this.startTime = startTime;
    this.eventTime = eventTime;
    this.state = state;
    this.serverIp = serverIp;
    this.serverConf = serverConf;
    this.serverEnv = serverEnv;
    this.buildInfo = buildInfo;
  }

  public String getServerName() {
    return serverName;
  }

  public void setServerName(String serverName) {
    this.serverName = serverName;
  }

  public Long getStartTime() {
    return startTime;
  }

  public void setStartTime(Long startTime) {
    this.startTime = startTime;
  }

  public Long getEventTime() {
    return eventTime;
  }

  public void setEventTime(Long eventTime) {
    this.eventTime = eventTime;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getServerIp() {
    return serverIp;
  }

  public void setServerIp(String serverIp) {
    this.serverIp = serverIp;
  }

  public Map<String, String> getServerConf() {
    if (null == serverConf) {
      return Collections.emptyMap();
    }
    return serverConf;
  }

  public void setServerConf(Map<String, String> serverConf) {
    this.serverConf = serverConf;
  }

  public Map<String, String> getServerEnv() {
    if (null == serverEnv) {
      return Collections.emptyMap();
    }
    return serverEnv;
  }

  public void setServerEnv(Map<String, String> serverEnv) {
    this.serverEnv = serverEnv;
  }

  public Map<String, String> getBuildInfo() {
    if (null == buildInfo) {
      return Collections.emptyMap();
    }
    return buildInfo;
  }

  public void setBuildInfo(Map<String, String> buildInfo) {
    this.buildInfo = buildInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KyuubiServerEvent that = (KyuubiServerEvent) o;
    return Objects.equals(getServerName(), that.getServerName())
        && Objects.equals(getStartTime(), that.getStartTime())
        && Objects.equals(getEventTime(), that.getEventTime())
        && Objects.equals(getState(), that.getState())
        && Objects.equals(getServerIp(), that.getServerIp())
        && Objects.equals(getServerConf(), that.getServerConf())
        && Objects.equals(getServerEnv(), that.getServerEnv())
        && Objects.equals(getBuildInfo(), that.getBuildInfo());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getServerName(),
        getStartTime(),
        getEventTime(),
        getState(),
        getServerIp(),
        getServerConf(),
        getServerEnv(),
        getBuildInfo());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
