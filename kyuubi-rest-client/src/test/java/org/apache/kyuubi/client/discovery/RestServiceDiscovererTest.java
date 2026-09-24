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

/**
 * Unit tests for RestServiceDiscoverer. Integration tests with embedded ZK are covered by {@code
 * KyuubiRestServiceDiscoverySuite} in kyuubi-server.
 */
public class RestServiceDiscovererTest {

  @Test(expected = IllegalArgumentException.class)
  public void testDiscoverValidatesBlankAddresses() {
    RestDiscoveryConf conf = new RestDiscoveryConf().setHaNamespace("kyuubi");
    // haAddresses is blank, should fail validation
    RestServiceDiscoverer.discoverHostUrls(conf);
  }

  @Test
  public void testRestNamespaceConstruction() {
    // Verify the namespace logic: haNamespace + "/rest"
    RestDiscoveryConf conf =
        new RestDiscoveryConf().setHaAddresses("localhost:2181").setHaNamespace("kyuubi");
    // The namespace should be "kyuubi/rest" — this is tested via the
    // server-side KyuubiRestServiceDiscoverySuite integration test
    assertEquals("kyuubi", conf.getHaNamespace());
  }

  @Test
  public void testCustomNamespace() {
    RestDiscoveryConf conf =
        new RestDiscoveryConf().setHaAddresses("zk1:2181").setHaNamespace("custom-ns");
    assertEquals("custom-ns", conf.getHaNamespace());
  }

  @Test
  public void testRefreshIntervalDefault() {
    RestDiscoveryConf conf = new RestDiscoveryConf().setHaAddresses("zk1:2181");
    assertEquals(5000L, conf.getRefreshIntervalMs());
  }

  @Test
  public void testRefreshIntervalCustom() {
    RestDiscoveryConf conf =
        new RestDiscoveryConf().setHaAddresses("zk1:2181").setRefreshIntervalMs(10000L);
    assertEquals(10000L, conf.getRefreshIntervalMs());
  }
}
