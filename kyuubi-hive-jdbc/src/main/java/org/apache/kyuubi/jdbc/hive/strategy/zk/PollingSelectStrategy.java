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

import java.util.List;
import org.apache.kyuubi.jdbc.hive.strategy.ServerSelectStrategy;
import org.apache.kyuubi.shaded.curator.framework.CuratorFramework;
import org.apache.kyuubi.shaded.curator.framework.recipes.atomic.AtomicValue;
import org.apache.kyuubi.shaded.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.kyuubi.shaded.curator.retry.RetryForever;

public class PollingSelectStrategy implements ServerSelectStrategy {
  public static final String strategyName = "polling";

  private static final String COUNTER_PATH_PREFIX = "/";
  private static final String COUNTER_PATH_SUFFIX = "-counter";

  @Override
  public String chooseServer(
      List<String> serverHosts, CuratorFramework zkClient, String namespace) {
    String counterPath = COUNTER_PATH_PREFIX + namespace + COUNTER_PATH_SUFFIX;
    try {
      return serverHosts.get(getAndIncrement(zkClient, counterPath) % serverHosts.size());
    } catch (Exception e) {
      throw new RuntimeException("Failed to choose server by polling select strategy", e);
    }
  }

  private int getAndIncrement(CuratorFramework zkClient, String path) throws Exception {
    DistributedAtomicInteger dai =
        new DistributedAtomicInteger(zkClient, path, new RetryForever(3000));
    AtomicValue<Integer> atomicVal;
    do {
      atomicVal = dai.add(1);
    } while (atomicVal == null || !atomicVal.succeeded());
    return atomicVal.preValue();
  }
}
