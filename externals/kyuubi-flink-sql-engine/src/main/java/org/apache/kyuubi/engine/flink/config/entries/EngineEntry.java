/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.engine.flink.config.entries;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.kyuubi.engine.flink.config.EngineEnvironment;

/** Describes a engine configuration entry. */
public class EngineEntry extends ConfigEntry {

  public static final EngineEntry DEFAULT_INSTANCE =
      new EngineEntry(new DescriptorProperties(true));

  private static final String ENGINE_BIND_ADDRESS = "bind-address";

  private static final String ENGINE_ADDRESS = "address";

  private static final String ENGINE_PORT = "port";

  private static final String JVM_ARGS = "jvm_args";

  private EngineEntry(DescriptorProperties properties) {
    super(properties);
  }

  @Override
  protected void validate(DescriptorProperties properties) {
    properties.validateString(ENGINE_BIND_ADDRESS, true);
    properties.validateString(ENGINE_ADDRESS, true);
    properties.validateInt(ENGINE_PORT, true, 1024, 65535);
    properties.validateString(JVM_ARGS, true);
  }

  public Map<String, String> asTopLevelMap() {
    return properties.asPrefixedMap(EngineEnvironment.ENGINE_ENTRY + '.');
  }

  /**
   * Merges two session entries. The properties of the first execution entry might be overwritten by
   * the second one.
   */
  public static EngineEntry merge(EngineEntry engine1, EngineEntry engine2) {
    final Map<String, String> mergedProperties = new HashMap<>(engine1.asTopLevelMap());
    mergedProperties.putAll(engine2.asTopLevelMap());

    final DescriptorProperties properties = new DescriptorProperties(true);
    properties.putProperties(mergedProperties);

    return new EngineEntry(properties);
  }
}
