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
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kyuubi.jdbc.hive.strategy.ChooseServerStrategy;
import org.apache.kyuubi.shaded.curator.framework.CuratorFramework;

public class RandomChooseStrategy implements ChooseServerStrategy {
  @Override
  public String chooseServer(
      List<String> serverHosts, CuratorFramework zkClient, String namespace) {
    return serverHosts.get(ThreadLocalRandom.current().nextInt(serverHosts.size()));
  }
}
