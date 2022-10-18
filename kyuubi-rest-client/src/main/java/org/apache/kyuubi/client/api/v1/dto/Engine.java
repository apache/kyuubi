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

public class Engine {

  private String version;
  private String user;
  private String engineType;
  private String sharelevel;
  private String subdomain;
  private String instance;

  public Engine() {}

  public Engine(
      String version,
      String user,
      String engineType,
      String sharelevel,
      String subdomain,
      String instance) {
    this.version = version;
    this.user = user;
    this.engineType = engineType;
    this.sharelevel = sharelevel;
    this.subdomain = subdomain;
    this.instance = instance;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getEngineType() {
    return engineType;
  }

  public void setEngineType(String engineType) {
    this.engineType = engineType;
  }

  public String getSharelevel() {
    return sharelevel;
  }

  public void setSharelevel(String sharelevel) {
    this.sharelevel = sharelevel;
  }

  public String getSubdomain() {
    return subdomain;
  }

  public void setSubdomain(String subdomain) {
    this.subdomain = subdomain;
  }

  public String getInstance() {
    return instance;
  }

  public void setInstance(String instance) {
    this.instance = instance;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Engine that = (Engine) o;
    return Objects.equals(getVersion(), that.getVersion())
        && Objects.equals(getUser(), that.getUser())
        && Objects.equals(getEngineType(), that.getEngineType())
        && Objects.equals(getSharelevel(), that.getSharelevel())
        && Objects.equals(getSubdomain(), that.getSubdomain())
        && Objects.equals(getInstance(), that.getInstance());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getVersion(), getUser(), getEngineType(), getSharelevel(), getSubdomain(), getInstance());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
