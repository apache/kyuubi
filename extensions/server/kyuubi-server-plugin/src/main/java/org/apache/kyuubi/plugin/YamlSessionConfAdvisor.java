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

package org.apache.kyuubi.plugin;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kyuubi.config.KyuubiConf;
import org.yaml.snakeyaml.Yaml;

public class YamlSessionConfAdvisor implements SessionConfAdvisor {
  @Override
  public Map<String, String> getConfOverlay(String user, Map<String, String> sessionConf) {
    HashMap<String, String> userConf = new HashMap<>();
    File file = new File("conf/engine-cluster-env.yaml");
    HashMap<String, HashMap<String, HashMap<String, String>>> yamlConf = new HashMap<>();

    Yaml yaml = new Yaml();
    FileInputStream fileInputStream = null;
    try {
      fileInputStream = new FileInputStream(file);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
    yamlConf = yaml.loadAs(fileInputStream, yamlConf.getClass());

    String clusterName = sessionConf.get(KyuubiConf.ENGINE_CLUSTER_NAME().key());
    if (!yamlConf.containsKey(clusterName)) {
      throw new RuntimeException(clusterName + " is an invalid engine cluster name!");
    }
    HashMap<String, HashMap<String, String>> sessionMap = yamlConf.get(clusterName);
    if (sessionMap == null) {
      return userConf;
    }
    HashMap<String, String> confMap = sessionMap.get("conf");
    if (confMap != null) {
      userConf.putAll(confMap);
    }
    HashMap<String, String> envMap = sessionMap.get("env");
    if (envMap != null) {
      envMap
          .entrySet()
          .forEach(
              e ->
                  userConf.put(
                      KyuubiConf.KYUUBI_ENGINE_ENV_PREFIX() + "." + e.getKey(), e.getValue()));
    }
    return userConf;
  }
}
