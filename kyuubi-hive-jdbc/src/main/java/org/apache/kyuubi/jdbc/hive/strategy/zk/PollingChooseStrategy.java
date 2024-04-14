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

package org.apache.kyuubi.jdbc.hive.strategy.zk;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kyuubi.jdbc.hive.ZooKeeperHiveClientException;
import org.apache.kyuubi.jdbc.hive.strategy.ChooseServerStrategy;
import org.apache.kyuubi.shaded.curator.framework.CuratorFramework;
import org.apache.kyuubi.shaded.curator.framework.recipes.atomic.AtomicValue;
import org.apache.kyuubi.shaded.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.kyuubi.shaded.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.kyuubi.shaded.curator.retry.RetryForever;
import org.apache.kyuubi.shaded.zookeeper.CreateMode;

public class PollingChooseStrategy implements ChooseServerStrategy {
  private static final String COUNTER_PATH_PREFIX = "/";
  private static final String COUNTER_PATH_SUFFIX = "-counter";
  private static final int COUNTER_RESET_VALUE = 1000;

  @Override
  public String chooseServer(List<String> serverHosts, CuratorFramework zkClient, String namespace)
      throws ZooKeeperHiveClientException {
    String counterPath = COUNTER_PATH_PREFIX + namespace + COUNTER_PATH_SUFFIX;
    InterProcessSemaphoreMutex lock = new InterProcessSemaphoreMutex(zkClient, counterPath);
    try {
      if (zkClient.checkExists().forPath(counterPath) == null) {
        zkClient
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(counterPath, "0".getBytes(StandardCharsets.UTF_8));
      }
      if (!lock.acquire(60, TimeUnit.SECONDS)) {
        return null;
      }
      return serverHosts.get(getAndIncrement(zkClient, counterPath) % serverHosts.size());
    } catch (Exception e) {
      throw new ZooKeeperHiveClientException(
          "Oops, PollingChooseStrategy get the server is wrong!", e);
    } finally {
      try {
        lock.release();
      } catch (Exception e) {
        throw new ZooKeeperHiveClientException(
            "Oops,PollingChooseStrategy releasing lock is wrong!", e);
      }
    }
  }

  private int getAndIncrement(CuratorFramework zkClient, String path) throws Exception {
    DistributedAtomicInteger dai =
        new DistributedAtomicInteger(zkClient, path, new RetryForever(1000));
    AtomicValue<Integer> atomicVal;
    do {
      atomicVal = dai.add(1);
    } while (atomicVal == null || !atomicVal.succeeded());
    dai.trySet(atomicVal.postValue() % COUNTER_RESET_VALUE);
    return atomicVal.preValue();
  }
}
