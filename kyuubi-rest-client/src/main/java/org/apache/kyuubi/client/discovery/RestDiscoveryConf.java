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

package org.apache.kyuubi.client.discovery;

import org.apache.commons.lang3.StringUtils;

/**
 * Configuration for REST service discovery via ZK/ETCD.
 *
 * <p>Example usage:
 *
 * <pre>
 *   RestDiscoveryConf conf = new RestDiscoveryConf()
 *       .setHaAddresses("zk1:2181,zk2:2181")
 *       .setHaNamespace("kyuubi");
 * </pre>
 *
 * <p>The REST namespace is derived as {@code haNamespace + "/rest"}, e.g. "kyuubi/rest" for the
 * default namespace.
 */
public class RestDiscoveryConf {

  private String haAddresses;
  private String haNamespace = "kyuubi";
  private String haClientClass = "org.apache.kyuubi.ha.client.zookeeper.ZookeeperDiscoveryClient";
  private long refreshIntervalMs = 5000L;

  public String getHaAddresses() {
    return haAddresses;
  }

  public RestDiscoveryConf setHaAddresses(String haAddresses) {
    this.haAddresses = haAddresses;
    return this;
  }

  public String getHaNamespace() {
    return haNamespace;
  }

  public RestDiscoveryConf setHaNamespace(String haNamespace) {
    this.haNamespace = haNamespace;
    return this;
  }

  public String getHaClientClass() {
    return haClientClass;
  }

  public RestDiscoveryConf setHaClientClass(String haClientClass) {
    this.haClientClass = haClientClass;
    return this;
  }

  public long getRefreshIntervalMs() {
    return refreshIntervalMs;
  }

  public RestDiscoveryConf setRefreshIntervalMs(long refreshIntervalMs) {
    this.refreshIntervalMs = refreshIntervalMs;
    return this;
  }

  public void validate() {
    if (StringUtils.isBlank(haAddresses)) {
      throw new IllegalArgumentException("haAddresses must not be blank.");
    }
    if (StringUtils.isBlank(haNamespace)) {
      throw new IllegalArgumentException("haNamespace must not be blank.");
    }
    if (StringUtils.isBlank(haClientClass)) {
      throw new IllegalArgumentException("haClientClass must not be blank.");
    }
  }
}
