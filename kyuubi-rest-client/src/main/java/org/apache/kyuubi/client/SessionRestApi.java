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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.kyuubi.client.api.v1.dto.*;
import org.apache.kyuubi.client.util.JsonUtils;

public class SessionRestApi {

  private KyuubiRestClient client;

  private static final String API_BASE_PATH = "sessions";

  private SessionRestApi() {}

  public SessionRestApi(KyuubiRestClient client) {
    this.client = client;
  }

  public List<SessionData> listSessions() {
    SessionData[] result =
        this.getClient()
            .get(API_BASE_PATH, new HashMap<>(), SessionData[].class, client.getAuthHeader());
    return Arrays.asList(result);
  }

  public SessionHandle openSession(SessionOpenRequest sessionOpenRequest) {
    return this.getClient()
        .post(
            API_BASE_PATH,
            JsonUtils.toJson(sessionOpenRequest),
            SessionHandle.class,
            client.getAuthHeader());
  }

  public String closeSession(String sessionHandleStr) {
    String path = String.format("%s/%s", API_BASE_PATH, sessionHandleStr);
    return this.getClient().delete(path, new HashMap<>(), client.getAuthHeader());
  }

  public KyuubiSessionEvent getSessionEvent(String sessionHandleStr) {
    String path = String.format("%s/%s", API_BASE_PATH, sessionHandleStr);
    return this.getClient()
        .get(path, new HashMap<>(), KyuubiSessionEvent.class, client.getAuthHeader());
  }

  public InfoDetail getSessionInfo(String sessionHandleStr, int infoType) {
    String path = String.format("%s/%s/info/%s", API_BASE_PATH, sessionHandleStr, infoType);
    return this.getClient().get(path, new HashMap<>(), InfoDetail.class, client.getAuthHeader());
  }

  public int getOpenSessionCount() {
    String path = String.format("%s/count", API_BASE_PATH);
    return this.getClient()
        .get(path, new HashMap<>(), SessionOpenCount.class, client.getAuthHeader())
        .getOpenSessionCount();
  }

  public ExecPoolStatistic getExecPoolStatistic() {
    String path = String.format("%s/execPool/statistic", API_BASE_PATH);
    return this.getClient()
        .get(path, new HashMap<>(), ExecPoolStatistic.class, client.getAuthHeader());
  }

  public OperationHandle executeStatement(String sessionHandleStr, StatementRequest request) {
    String path = String.format("%s/%s/operations/statement", API_BASE_PATH, sessionHandleStr);
    return this.getClient()
        .post(path, JsonUtils.toJson(request), OperationHandle.class, client.getAuthHeader());
  }

  public OperationHandle getTypeInfo(String sessionHandleStr) {
    String path = String.format("%s/%s/operations/typeInfo", API_BASE_PATH, sessionHandleStr);
    return this.getClient().post(path, "", OperationHandle.class, client.getAuthHeader());
  }

  public OperationHandle getCatalogs(String sessionHandleStr) {
    String path = String.format("%s/%s/operations/catalogs", API_BASE_PATH, sessionHandleStr);
    return this.getClient().post(path, "", OperationHandle.class, client.getAuthHeader());
  }

  public OperationHandle getSchemas(String sessionHandleStr, GetSchemasRequest request) {
    String path = String.format("%s/%s/operations/schemas", API_BASE_PATH, sessionHandleStr);
    return this.getClient()
        .post(path, JsonUtils.toJson(request), OperationHandle.class, client.getAuthHeader());
  }

  public OperationHandle getTables(String sessionHandleStr, GetTablesRequest request) {
    String path = String.format("%s/%s/operations/tables", API_BASE_PATH, sessionHandleStr);
    return this.getClient()
        .post(path, JsonUtils.toJson(request), OperationHandle.class, client.getAuthHeader());
  }

  public OperationHandle getTableTypes(String sessionHandleStr) {
    String path = String.format("%s/%s/operations/tableTypes", API_BASE_PATH, sessionHandleStr);
    return this.getClient().post(path, "", OperationHandle.class, client.getAuthHeader());
  }

  public OperationHandle getColumns(String sessionHandleStr, GetColumnsRequest request) {
    String path = String.format("%s/%s/operations/columns", API_BASE_PATH, sessionHandleStr);
    return this.getClient()
        .post(path, JsonUtils.toJson(request), OperationHandle.class, client.getAuthHeader());
  }

  public OperationHandle getFunctions(String sessionHandleStr, GetFunctionsRequest request) {
    String path = String.format("%s/%s/operations/functions", API_BASE_PATH, sessionHandleStr);
    return this.getClient()
        .post(path, JsonUtils.toJson(request), OperationHandle.class, client.getAuthHeader());
  }

  public OperationHandle getPrimaryKeys(String sessionHandleStr, GetPrimaryKeysRequest request) {
    String path = String.format("%s/%s/operations/primaryKeys", API_BASE_PATH, sessionHandleStr);
    return this.getClient()
        .post(path, JsonUtils.toJson(request), OperationHandle.class, client.getAuthHeader());
  }

  public OperationHandle getCrossReference(
      String sessionHandleStr, GetCrossReferenceRequest request) {
    String path = String.format("%s/%s/operations/crossReference", API_BASE_PATH, sessionHandleStr);
    return this.getClient()
        .post(path, JsonUtils.toJson(request), OperationHandle.class, client.getAuthHeader());
  }

  private IRestClient getClient() {
    return this.client.getHttpClient();
  }
}
