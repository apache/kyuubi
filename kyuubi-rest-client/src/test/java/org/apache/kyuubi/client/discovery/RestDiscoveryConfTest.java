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

import static org.junit.Assert.*;

import org.junit.Test;

public class RestDiscoveryConfTest {

  @Test
  public void testDefaults() {
    RestDiscoveryConf conf = new RestDiscoveryConf();
    assertNull(conf.getHaAddresses());
    assertEquals("kyuubi", conf.getHaNamespace());
    assertEquals(
        "org.apache.kyuubi.ha.client.zookeeper.ZookeeperDiscoveryClient", conf.getHaClientClass());
    assertEquals(5000L, conf.getRefreshIntervalMs());
  }

  @Test
  public void testSetters() {
    RestDiscoveryConf conf =
        new RestDiscoveryConf()
            .setHaAddresses("zk1:2181,zk2:2181")
            .setHaNamespace("custom-ns")
            .setHaClientClass("org.apache.kyuubi.ha.client.etcd.EtcdDiscoveryClient")
            .setRefreshIntervalMs(10000L);

    assertEquals("zk1:2181,zk2:2181", conf.getHaAddresses());
    assertEquals("custom-ns", conf.getHaNamespace());
    assertEquals("org.apache.kyuubi.ha.client.etcd.EtcdDiscoveryClient", conf.getHaClientClass());
    assertEquals(10000L, conf.getRefreshIntervalMs());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateBlankAddresses() {
    new RestDiscoveryConf().validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateEmptyAddresses() {
    new RestDiscoveryConf().setHaAddresses("").validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateBlankNamespace() {
    new RestDiscoveryConf().setHaAddresses("zk1:2181").setHaNamespace("").validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateBlankClientClass() {
    new RestDiscoveryConf().setHaAddresses("zk1:2181").setHaClientClass("").validate();
  }

  @Test
  public void testValidateSuccess() {
    new RestDiscoveryConf().setHaAddresses("zk1:2181").validate();
  }
}
