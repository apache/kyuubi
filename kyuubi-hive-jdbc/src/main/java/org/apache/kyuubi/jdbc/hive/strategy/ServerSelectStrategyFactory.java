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

package org.apache.kyuubi.jdbc.hive.strategy;

import java.lang.reflect.Constructor;
import org.apache.kyuubi.jdbc.hive.strategy.zk.PollingSelectStrategy;
import org.apache.kyuubi.jdbc.hive.strategy.zk.RandomSelectStrategy;

public class ServerSelectStrategyFactory {
  public static ServerSelectStrategy createStrategy(String strategyName) {
    try {
      switch (strategyName) {
        case PollingSelectStrategy.strategyName:
          return new PollingSelectStrategy();
        case RandomSelectStrategy.strategyName:
          return new RandomSelectStrategy();
        default:
          Class<?> clazz = Class.forName(strategyName);
          if (ServerSelectStrategy.class.isAssignableFrom(clazz)) {
            Constructor<? extends ServerSelectStrategy> constructor =
                clazz.asSubclass(ServerSelectStrategy.class).getConstructor();
            return constructor.newInstance();
          } else {
            throw new ClassNotFoundException(
                "The loaded class does not implement ServerSelectStrategy");
          }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to init server select strategy", e);
    }
  }
}
