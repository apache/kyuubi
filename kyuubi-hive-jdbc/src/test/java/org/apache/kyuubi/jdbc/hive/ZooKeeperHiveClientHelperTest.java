/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.jdbc.hive;

import static org.apache.kyuubi.jdbc.hive.Utils.extractURLComponents;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Properties;
import org.apache.kyuubi.jdbc.hive.strategy.ServerSelectStrategyFactory;
import org.apache.kyuubi.jdbc.hive.strategy.zk.PollingSelectStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ZooKeeperHiveClientHelperTest {

  private static boolean rejectedClassInitialized;

  public static class RejectedStrategy {
    static {
      rejectedClassInitialized = true;
    }
  }

  @Test
  public void validateStrategyBeforeInitialization() {
    assertThrows(
        RuntimeException.class,
        () -> ServerSelectStrategyFactory.createStrategy(RejectedStrategy.class.getName()));
    assertFalse(rejectedClassInitialized);
    assertEquals(
        PollingSelectStrategy.class,
        ServerSelectStrategyFactory.createStrategy(PollingSelectStrategy.class.getName())
            .getClass());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc:hive2://hostname:10018/db;zooKeeperNamespace=zookeeper/namespace",
        "jdbc:hive2://hostname:10018/db;zooKeeperNamespace=/zookeeper/namespace",
        "jdbc:hive2://hostname:10018/db;zooKeeperNamespace=zookeeper/namespace/",
        "jdbc:hive2://hostname:10018/db;zooKeeperNamespace=/zookeeper/namespace/",
        "jdbc:hive2://hostname:10018/db;zooKeeperNamespace=///zookeeper/namespace///"
      })
  public void testGetZooKeeperNamespace(String uri) throws JdbcUriParseException {
    JdbcConnectionParams jdbcConnectionParams = extractURLComponents(uri, new Properties());
    assertEquals(
        "zookeeper/namespace",
        ZooKeeperHiveClientHelper.getZooKeeperNamespace(jdbcConnectionParams));
  }
}
