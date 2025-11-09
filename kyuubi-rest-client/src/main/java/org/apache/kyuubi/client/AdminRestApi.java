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

package org.apache.kyuubi.client;

import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.client.api.v1.dto.Engine;
import org.apache.kyuubi.client.api.v1.dto.KyuubiServerEvent;
import org.apache.kyuubi.client.api.v1.dto.OperationData;
import org.apache.kyuubi.client.api.v1.dto.ServerData;
import org.apache.kyuubi.client.api.v1.dto.SessionData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminRestApi {
  private static final Logger LOG = LoggerFactory.getLogger(AdminRestApi.class);

  private KyuubiRestClient client;

  private static final String API_BASE_PATH = "admin";

  private AdminRestApi() {}

  public AdminRestApi(KyuubiRestClient client) {
    this.client = client;
  }

  public String refreshHadoopConf() {
    String path = String.format("%s/%s", API_BASE_PATH, "refresh/hadoop_conf");
    return this.getClient().post(path, null, client.getAuthHeader());
  }

  public String refreshUserDefaultsConf() {
    String path = String.format("%s/%s", API_BASE_PATH, "refresh/user_defaults_conf");
    return this.getClient().post(path, null, client.getAuthHeader());
  }

  public String refreshKubernetesConf() {
    String path = String.format("%s/%s", API_BASE_PATH, "refresh/kubernetes_conf");
    return this.getClient().post(path, null, client.getAuthHeader());
  }

  public String refreshUnlimitedUsers() {
    String path = String.format("%s/%s", API_BASE_PATH, "refresh/unlimited_users");
    return this.getClient().post(path, null, client.getAuthHeader());
  }

  public String refreshDenyUsers() {
    String path = String.format("%s/%s", API_BASE_PATH, "refresh/deny_users");
    return this.getClient().post(path, null, client.getAuthHeader());
  }

  public String refreshDenyIps() {
    String path = String.format("%s/%s", API_BASE_PATH, "refresh/deny_ips");
    return this.getClient().post(path, null, client.getAuthHeader());
  }

  /** This method is deprecated since 1.10 */
  @Deprecated
  public String deleteEngine(
      String engineType, String shareLevel, String subdomain, String hs2ProxyUser) {
    LOG.warn(
        "The method `deleteEngine(engineType, shareLevel, subdomain, hs2ProxyUser)` "
            + "is deprecated since 1.10.0, using "
            + "`deleteEngine(engineType, shareLevel, subdomain, hs2ProxyUser, kill)` instead.");
    return this.deleteEngine(engineType, shareLevel, subdomain, hs2ProxyUser, false);
  }

  public String deleteEngine(
      String engineType, String shareLevel, String subdomain, String hs2ProxyUser, boolean kill) {
    Map<String, Object> params = new HashMap<>();
    params.put("type", engineType);
    params.put("sharelevel", shareLevel);
    params.put("subdomain", subdomain);
    params.put("hive.server2.proxy.user", hs2ProxyUser);
    params.put("kill", kill);
    return this.getClient().delete(API_BASE_PATH + "/engine", params, client.getAuthHeader());
  }

  public List<Engine> listEngines(
      String engineType, String shareLevel, String subdomain, String hs2ProxyUser) {
    Map<String, Object> params = new HashMap<>();
    params.put("type", engineType);
    params.put("sharelevel", shareLevel);
    params.put("subdomain", subdomain);
    params.put("hive.server2.proxy.user", hs2ProxyUser);
    Engine[] result =
        this.getClient()
            .get(API_BASE_PATH + "/engine", params, Engine[].class, client.getAuthHeader());
    return Arrays.asList(result);
  }

  public List<SessionData> listSessions() {
    return listSessions(Collections.emptyList(), null);
  }

  public List<SessionData> listSessions(List<String> users, String sessionType) {
    Map<String, Object> params = new HashMap<>();
    if (users != null && !users.isEmpty()) {
      params.put("users", String.join(",", users));
    }
    if (StringUtils.isNotBlank(sessionType)) {
      params.put("sessionType", sessionType);
    }
    SessionData[] result =
        this.getClient()
            .get(API_BASE_PATH + "/sessions", params, SessionData[].class, client.getAuthHeader());
    return Arrays.asList(result);
  }

  public String closeSession(String sessionHandleStr) {
    String url = String.format("%s/sessions/%s", API_BASE_PATH, sessionHandleStr);
    return this.getClient().delete(url, null, client.getAuthHeader());
  }

  public List<OperationData> listOperations() {
    return listOperations(Collections.emptyList(), null, null);
  }

  public List<OperationData> listOperations(
      List<String> users, String sessionHandleStr, String sessionType) {
    Map<String, Object> params = new HashMap<>();
    if (users != null && !users.isEmpty()) {
      params.put("users", String.join(",", users));
    }
    if (StringUtils.isNotBlank(sessionHandleStr)) {
      params.put("sessionHandle", sessionHandleStr);
    }
    if (StringUtils.isNotBlank(sessionType)) {
      params.put("sessionType", sessionType);
    }
    OperationData[] result =
        this.getClient()
            .get(
                API_BASE_PATH + "/operations",
                params,
                OperationData[].class,
                client.getAuthHeader());
    return Arrays.asList(result);
  }

  public String closeOperation(String operationHandleStr) {
    String url = String.format("%s/operations/%s", API_BASE_PATH, operationHandleStr);
    return this.getClient().delete(url, null, client.getAuthHeader());
  }

  public List<ServerData> listServers() {
    ServerData[] result =
        this.getClient()
            .get(API_BASE_PATH + "/server", null, ServerData[].class, client.getAuthHeader());
    return Arrays.asList(result);
  }

  public KyuubiServerEvent getServerEvent() {
    return this.getClient()
        .get(
            API_BASE_PATH + "/server/event", null, KyuubiServerEvent.class, client.getAuthHeader());
  }

  private IRestClient getClient() {
    return this.client.getHttpClient();
  }
}
