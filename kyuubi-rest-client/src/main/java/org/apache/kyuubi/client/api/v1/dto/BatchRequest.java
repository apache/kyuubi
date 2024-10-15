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

import java.util.*;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class BatchRequest {
  private String batchType;
  private String resource;
  private String className;
  private String name;
  private Map<String, String> conf = new HashMap<>(0);
  private List<String> args = new ArrayList<>(0);
  private Map<String, String> extraResourcesMap = new HashMap<>(0);

  public BatchRequest() {}

  public BatchRequest(
      String batchType,
      String resource,
      String className,
      String name,
      Map<String, String> conf,
      List<String> args) {
    this.batchType = batchType;
    this.resource = resource;
    this.className = className;
    this.name = name;
    this.conf = conf;
    this.args = args;
  }

  public BatchRequest(String batchType, String resource, String className, String name) {
    this.batchType = batchType;
    this.resource = resource;
    this.className = className;
    this.name = name;
  }

  public String getBatchType() {
    return batchType;
  }

  public void setBatchType(String batchType) {
    this.batchType = batchType;
  }

  public String getResource() {
    return resource;
  }

  public void setResource(String resource) {
    this.resource = resource;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Map<String, String> getConf() {
    if (null == conf) {
      return new HashMap<>(0);
    }
    return conf;
  }

  public void setConf(Map<String, String> conf) {
    this.conf = conf;
  }

  public List<String> getArgs() {
    if (null == args) {
      return new ArrayList<>(0);
    }
    return args;
  }

  public void setArgs(List<String> args) {
    this.args = args;
  }

  public Map<String, String> getExtraResourcesMap() {
    return extraResourcesMap;
  }

  public void setExtraResourcesMap(Map<String, String> extraResourcesMap) {
    this.extraResourcesMap = extraResourcesMap;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BatchRequest that = (BatchRequest) o;
    return Objects.equals(getBatchType(), that.getBatchType())
        && Objects.equals(getResource(), that.getResource())
        && Objects.equals(getClassName(), that.getClassName())
        && Objects.equals(getName(), that.getName())
        && Objects.equals(getConf(), that.getConf())
        && Objects.equals(getArgs(), that.getArgs())
        && Objects.equals(getExtraResourcesMap(), that.getExtraResourcesMap());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getBatchType(),
        getResource(),
        getClassName(),
        getName(),
        getConf(),
        getArgs(),
        getExtraResourcesMap());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
