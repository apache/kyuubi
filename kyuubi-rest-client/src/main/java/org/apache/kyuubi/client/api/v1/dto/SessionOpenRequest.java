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
  private Integer protocolVersion;
  private Map<String, String> configs;

  public SessionOpenRequest() {}

  public SessionOpenRequest(Map<String, String> configs) {
    this.configs = configs;
  }

  public SessionOpenRequest(Integer protocolVersion, Map<String, String> configs) {
    this.protocolVersion = protocolVersion;
    this.configs = configs;
  }

  public Integer getProtocolVersion() {
    return protocolVersion;
  }

  public void setProtocolVersion(Integer protocolVersion) {
    this.protocolVersion = protocolVersion;
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
    return Objects.equals(getProtocolVersion(), that.getProtocolVersion())
        && Objects.equals(getConfigs(), that.getConfigs());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProtocolVersion(), getConfigs());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
