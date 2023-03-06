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

public class Server {

  private String nodeName;
  private String namespace;
  private String instance;
  private String host;
  private int port;
  private Long createTime;
  private Long memoryTotal;
  private int cpuTotal;
  private String status;

  public Server(
      String nodeName,
      String namespace,
      String instance,
      String host,
      int port,
      Long createTime,
      Long memoryTotal,
      int cpuTotal,
      String status) {
    this.nodeName = nodeName;
    this.namespace = namespace;
    this.instance = instance;
    this.host = host;
    this.port = port;
    this.createTime = createTime;
    this.memoryTotal = memoryTotal;
    this.cpuTotal = cpuTotal;
    this.status = status;
  }

  public String getNodeName() {
    return nodeName;
  }

  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public String getInstance() {
    return instance;
  }

  public void setInstance(String instance) {
    this.instance = instance;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public Long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(Long createTime) {
    this.createTime = createTime;
  }

  public Long getMemoryTotal() {
    return memoryTotal;
  }

  public void setMemoryTotal(Long memoryTotal) {
    this.memoryTotal = memoryTotal;
  }

  public int getCpuTotal() {
    return cpuTotal;
  }

  public void setCpuTotal(int cpuTotal) {
    this.cpuTotal = cpuTotal;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Server server = (Server) o;
    return port == server.port
        && cpuTotal == server.cpuTotal
        && Objects.equals(nodeName, server.nodeName)
        && Objects.equals(namespace, server.namespace)
        && Objects.equals(instance, server.instance)
        && Objects.equals(host, server.host)
        && Objects.equals(createTime, server.createTime)
        && Objects.equals(memoryTotal, server.memoryTotal)
        && Objects.equals(status, server.status);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        nodeName, namespace, instance, host, port, createTime, memoryTotal, cpuTotal, status);
  }
}
