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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.kyuubi.config.KyuubiConf;
import org.apache.kyuubi.ha.client.DiscoveryClient;
import org.apache.kyuubi.ha.client.DiscoveryClientProvider;
import org.apache.kyuubi.ha.client.ServiceNodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility to discover REST service endpoints from ZK/ETCD. It creates a discovery client, reads
 * the service nodes under the given namespace, and returns the list of {@code http://host:port}
 * URLs.
 */
public class RestServiceDiscoverer {

  private static final Logger LOG = LoggerFactory.getLogger(RestServiceDiscoverer.class);

  private RestServiceDiscoverer() {}

  /**
   * Discover REST service host URLs from the configured discovery service.
   *
   * @param discoveryConf the discovery configuration
   * @return list of host URLs in the form {@code http://host:port}
   */
  public static List<String> discoverHostUrls(RestDiscoveryConf discoveryConf) {
    discoveryConf.validate();
    KyuubiConf conf = toKyuubiConf(discoveryConf);
    DiscoveryClient client = DiscoveryClientProvider.createDiscoveryClient(conf);
    try {
      client.createClient();
      String restNamespace = discoveryConf.getHaNamespace() + "/rest";
      List<ServiceNodeInfo> nodes =
          scala.collection.JavaConverters.seqAsJavaListConverter(
                  client.getServiceNodesInfo(restNamespace, scala.Option.empty(), false))
              .asJava();
      if (nodes == null || nodes.isEmpty()) {
        throw new RuntimeException("No REST service nodes found under namespace: " + restNamespace);
      }
      List<String> urls =
          nodes.stream()
              .map(node -> "http://" + node.host() + ":" + node.port())
              .collect(Collectors.toList());
      LOG.info("Discovered REST service endpoints: {}", urls);
      return urls;
    } finally {
      try {
        client.closeClient();
      } catch (Exception e) {
        LOG.warn("Failed to close discovery client", e);
      }
    }
  }

  private static KyuubiConf toKyuubiConf(RestDiscoveryConf discoveryConf) {
    KyuubiConf conf = new KyuubiConf(true);
    conf.set("kyuubi.ha.addresses", discoveryConf.getHaAddresses());
    conf.set("kyuubi.ha.client.class", discoveryConf.getHaClientClass());
    return conf;
  }
}
