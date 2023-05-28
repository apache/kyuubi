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

public class ServerData {
  private String nodeName;
  private String namespace;
  private String instance;
  private String host;
  private int port;
  private Map<String, String> attributes;
  private String status;

  public ServerData() {}

  public ServerData(
      String nodeName,
      String namespace,
      String instance,
      String host,
      int port,
      Map<String, String> attributes,
      String status) {
    this.nodeName = nodeName;
    this.namespace = namespace;
    this.instance = instance;
    this.host = host;
    this.port = port;
    this.attributes = attributes;
    this.status = status;
  }

  public String getNodeName() {
    return nodeName;
  }

  public ServerData setNodeName(String nodeName) {
    this.nodeName = nodeName;
    return this;
  }

  public String getNamespace() {
    return namespace;
  }

  public ServerData setNamespace(String namespace) {
    this.namespace = namespace;
    return this;
  }

  public String getInstance() {
    return instance;
  }

  public ServerData setInstance(String instance) {
    this.instance = instance;
    return this;
  }

  public String getHost() {
    return host;
  }

  public ServerData setHost(String host) {
    this.host = host;
    return this;
  }

  public int getPort() {
    return port;
  }

  public ServerData setPort(int port) {
    this.port = port;
    return this;
  }

  public Map<String, String> getAttributes() {
    if (null == attributes) {
      return Collections.emptyMap();
    }
    return attributes;
  }

  public ServerData setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
    return this;
  }

  public String getStatus() {
    return status;
  }

  public ServerData setStatus(String status) {
    this.status = status;
    return this;
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeName, namespace, instance, port, attributes, status);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    ServerData server = (ServerData) obj;
    return port == server.port
        && Objects.equals(nodeName, server.nodeName)
        && Objects.equals(namespace, server.namespace)
        && Objects.equals(instance, server.instance)
        && Objects.equals(host, server.host)
        && Objects.equals(status, server.status);
  }
}
