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

package org.apache.kyuubi.jdbc.hive.server;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hive.registry.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceInstanceBase implements ServiceInstance {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceInstanceBase.class);
  private String host;
  private int rpcPort;
  private String workerIdentity;
  private Map<String, String> properties;

  public ServiceInstanceBase() {}

  public ServiceInstanceBase(ServiceRecord srv, String rpcName) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Working with ServiceRecord: {}", srv);
    }

    Endpoint rpc = srv.getInternalEndpoint(rpcName);
    this.host = RegistryTypeUtils.getAddressField((Map) rpc.addresses.get(0), "host");
    this.rpcPort =
        Integer.parseInt(RegistryTypeUtils.getAddressField((Map) rpc.addresses.get(0), "port"));
    this.workerIdentity = srv.get("registry.unique.id");
    this.properties = srv.attributes();
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      ServiceInstanceBase other = (ServiceInstanceBase) o;
      return Objects.equals(this.getWorkerIdentity(), other.getWorkerIdentity())
          && Objects.equals(this.host, other.host)
          && Objects.equals(this.rpcPort, other.rpcPort);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return this.getWorkerIdentity().hashCode() + 31 * this.host.hashCode() + 31 * this.rpcPort;
  }

  public String getWorkerIdentity() {
    return this.workerIdentity;
  }

  public String getHost() {
    return this.host;
  }

  public int getRpcPort() {
    return this.rpcPort;
  }

  public Map<String, String> getProperties() {
    return this.properties;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public void setRpcPort(int rpcPort) {
    this.rpcPort = rpcPort;
  }

  public void setWorkerIdentity(String workerIdentity) {
    this.workerIdentity = workerIdentity;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public String toString() {
    return "DynamicServiceInstance [id="
        + this.getWorkerIdentity()
        + ", host="
        + this.host
        + ":"
        + this.rpcPort
        + "]";
  }
}
