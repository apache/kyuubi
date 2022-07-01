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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.utils.CloseableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HS2ActivePassiveHARegistry extends ZkRegistryBase<HiveServer2Instance>
    implements ServiceRegistry<HiveServer2Instance>, HiveServer2HAInstanceSet {
  private static final Logger LOG = LoggerFactory.getLogger(HS2ActivePassiveHARegistry.class);
  static final String ACTIVE_ENDPOINT = "activeEndpoint";
  static final String PASSIVE_ENDPOINT = "passiveEndpoint";
  private static final String INSTANCE_PREFIX = "instance-";
  private static final String INSTANCE_GROUP = "instances";
  private static final String LEADER_LATCH_PATH = "/_LEADER";
  private LeaderLatch leaderLatch;
  private String latchPath;
  private final String uniqueId;

  // There are 2 paths under which the instances get registered
  // 1) Standard path used by ZkRegistryBase where all instances register themselves (also stores
  // metadata)
  // Secure: /hs2ActivePassiveHA-sasl/instances/instance-0000000000
  // Unsecure: /hs2ActivePassiveHA-unsecure/instances/instance-0000000000
  // 2) Leader latch path used for HS2 HA Active/Passive configuration where all instances register
  // under _LEADER
  //    path but only one among them is the leader
  // Secure: /hs2ActivePassiveHA-sasl/_LEADER/xxxx-latch-0000000000
  // Unsecure: /hs2ActivePassiveHA-unsecure/_LEADER/xxxx-latch-0000000000
  static HS2ActivePassiveHARegistry create(Configuration conf) {
    String zkNameSpace =
        HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_REGISTRY_NAMESPACE);
    Preconditions.checkArgument(
        !StringUtils.isBlank(zkNameSpace),
        HiveConf.ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_REGISTRY_NAMESPACE.varname
            + " cannot be null or empty");
    String principal = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL);
    String keytab = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB);
    String zkNameSpacePrefix = zkNameSpace + "-";
    return new HS2ActivePassiveHARegistry(
        null, zkNameSpacePrefix, LEADER_LATCH_PATH, principal, keytab, null, conf);
  }

  private HS2ActivePassiveHARegistry(
      final String instanceName,
      final String zkNamespacePrefix,
      final String leaderLatchPath,
      final String krbPrincipal,
      final String krbKeytab,
      final String saslContextName,
      final Configuration conf) {
    super(
        instanceName,
        conf,
        null,
        zkNamespacePrefix,
        null,
        INSTANCE_PREFIX,
        INSTANCE_GROUP,
        saslContextName,
        krbPrincipal,
        krbKeytab,
        null);
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_IN_TEST)
        && conf.get(ZkRegistryBase.UNIQUE_IDENTIFIER) != null) {
      this.uniqueId = conf.get(ZkRegistryBase.UNIQUE_IDENTIFIER);
    } else {
      this.uniqueId = UNIQUE_ID.toString();
    }
    this.latchPath = leaderLatchPath;
    this.leaderLatch = getNewLeaderLatchPath();
  }

  @Override
  public void start() throws IOException {
    super.start();
    populateCache();
    LOG.info("Populating instances cache for client");
  }

  private void populateCache() throws IOException {
    PathChildrenCache pcc = ensureInstancesCache(0);
    populateCache(pcc, false);
  }

  @Override
  public ServiceInstanceSet<HiveServer2Instance> getInstances(
      final String component, final long clusterReadyTimeoutMs) throws IOException {
    throw new IOException("Not supported to get instances by component name");
  }

  @Override
  public void stop() {
    CloseableUtils.closeQuietly(leaderLatch);
    super.stop();
  }

  @Override
  protected HiveServer2Instance createServiceInstance(final ServiceRecord srv) throws IOException {
    Endpoint activeEndpoint = srv.getInternalEndpoint(HS2ActivePassiveHARegistry.ACTIVE_ENDPOINT);
    return new HiveServer2Instance(
        srv, activeEndpoint != null ? ACTIVE_ENDPOINT : PASSIVE_ENDPOINT);
  }

  @Override
  public synchronized void registerStateChangeListener(
      final ServiceInstanceStateChangeListener<HiveServer2Instance> listener) throws IOException {
    super.registerStateChangeListener(listener);
  }

  @Override
  public ApplicationId getApplicationId() throws IOException {
    throw new IOException("Not supported until HS2 runs as YARN application");
  }

  @Override
  protected String getZkPathUser(final Configuration conf) {
    return RegistryUtils.currentUser();
  }

  /**
   * Returns a new instance of leader latch path but retains the same uniqueId. This is only used
   * when HS2 startsup or when a manual failover is triggered (in which case uniqueId will still
   * remain as the instance has not restarted)
   *
   * @return - new leader latch
   */
  private LeaderLatch getNewLeaderLatchPath() {
    return new LeaderLatch(
        zooKeeperClient, latchPath, uniqueId, LeaderLatch.CloseMode.NOTIFY_LEADER);
  }

  @Override
  public HiveServer2Instance getLeader() {
    for (HiveServer2Instance hs2Instance : getAll()) {
      if (hs2Instance.isLeader()) {
        return hs2Instance;
      }
    }
    return null;
  }

  @Override
  public Collection<HiveServer2Instance> getAll() {
    return getAllInternal();
  }

  @Override
  public HiveServer2Instance getInstance(final String instanceId) {
    for (HiveServer2Instance hs2Instance : getAll()) {
      if (hs2Instance.getWorkerIdentity().equals(instanceId)) {
        return hs2Instance;
      }
    }
    return null;
  }

  @Override
  public Set<HiveServer2Instance> getByHost(final String host) {
    return getByHostInternal(host);
  }

  @Override
  public int size() {
    return sizeInternal();
  }
}
