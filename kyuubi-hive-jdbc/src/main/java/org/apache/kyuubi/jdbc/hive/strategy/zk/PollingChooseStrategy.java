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
import org.apache.kyuubi.shaded.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.kyuubi.shaded.zookeeper.CreateMode;

public class PollingChooseStrategy implements ChooseServerStrategy {
  private static final String COUNTER_PATH_PREFIX = "/";
  private static final String COUNTER_PATH_SUFFIX = "-counter";
  private static final int COUNTER_RESET_VALUE = 1000;

  @Override
  public String chooseServer(List<String> serverHosts, CuratorFramework zkClient, String namespace)
      throws ZooKeeperHiveClientException {
    String counter_path = COUNTER_PATH_PREFIX + namespace + COUNTER_PATH_SUFFIX;
    InterProcessSemaphoreMutex lock = new InterProcessSemaphoreMutex(zkClient, counter_path);
    try {
      if (zkClient.checkExists().forPath(counter_path) == null) {
        zkClient
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(counter_path, "1".getBytes(StandardCharsets.UTF_8));
      }
      if (!lock.acquire(60, TimeUnit.SECONDS)) {
        return null;
      }
      byte[] data = zkClient.getData().forPath(counter_path);
      String dataStr = new String(data, StandardCharsets.UTF_8);
      int counter = Integer.parseInt(dataStr);
      String server = serverHosts.get(counter % serverHosts.size());
      counter = (counter + 1) % COUNTER_RESET_VALUE; // 避免计数器溢出
      zkClient.setData().forPath(counter_path, (counter + "").getBytes(StandardCharsets.UTF_8));
      return server;
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
}
