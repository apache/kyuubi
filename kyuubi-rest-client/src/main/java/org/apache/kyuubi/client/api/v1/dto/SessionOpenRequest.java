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

public class SessionOpenRequest {
  private int protocolVersion;
  private String user;
  private String password;
  private String ipAddr;
  private Map<String, String> configs;

  public SessionOpenRequest() {}

  public SessionOpenRequest(
      int protocolVersion,
      String user,
      String password,
      String ipAddr,
      Map<String, String> configs) {
    this.protocolVersion = protocolVersion;
    this.user = user;
    this.password = password;
    this.ipAddr = ipAddr;
    this.configs = configs;
  }

  public int getProtocolVersion() {
    return protocolVersion;
  }

  public void setProtocolVersion(int protocolVersion) {
    this.protocolVersion = protocolVersion;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getIpAddr() {
    return ipAddr;
  }

  public void setIpAddr(String ipAddr) {
    this.ipAddr = ipAddr;
  }

  public Map<String, String> getConfigs() {
    if (null == configs) {
      return Collections.emptyMap();
    }
    return configs;
  }

  public void setConfigs(Map<String, String> configs) {
    this.configs = configs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SessionOpenRequest that = (SessionOpenRequest) o;
    return getProtocolVersion() == that.getProtocolVersion()
        && Objects.equals(getUser(), that.getUser())
        && Objects.equals(getPassword(), that.getPassword())
        && Objects.equals(getIpAddr(), that.getIpAddr())
        && Objects.equals(getConfigs(), that.getConfigs());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProtocolVersion(), getUser(), getPassword(), getIpAddr(), getConfigs());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
