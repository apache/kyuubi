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

import java.util.HashMap;
import java.util.Map;
import org.apache.kyuubi.client.api.v1.dto.*;
import org.apache.kyuubi.client.util.JsonUtils;

public class OperationRestApi {

  private KyuubiRestClient client;

  private static final String API_BASE_PATH = "operations";

  private OperationRestApi() {}

  public OperationRestApi(KyuubiRestClient client) {
    this.client = client;
  }

  public KyuubiOperationEvent getOperationEvent(String operationHandleStr) {
    String path = String.format("%s/%s/event", API_BASE_PATH, operationHandleStr);
    return this.getClient()
        .get(path, new HashMap<>(), KyuubiOperationEvent.class, client.getAuthHeader());
  }

  public String applyOperationAction(OpActionRequest request, String operationHandleStr) {
    String path = String.format("%s/%s", API_BASE_PATH, operationHandleStr);
    return this.getClient().put(path, JsonUtils.toJson(request), client.getAuthHeader());
  }

  public ResultSetMetaData getResultSetMetadata(String operationHandleStr) {
    String path = String.format("%s/%s/resultsetmetadata", API_BASE_PATH, operationHandleStr);
    return this.getClient()
        .get(path, new HashMap<>(), ResultSetMetaData.class, client.getAuthHeader());
  }

  public OperationLog getOperationLog(String operationHandleStr, int maxRows) {
    String path = String.format("%s/%s/log", API_BASE_PATH, operationHandleStr);
    Map<String, Object> params = new HashMap<>();
    params.put("maxrows", maxRows);
    return this.getClient().get(path, params, OperationLog.class, client.getAuthHeader());
  }

  public ResultRowSet getNextRowSet(String operationHandleStr) {
    return getNextRowSet(operationHandleStr, null, null);
  }

  public ResultRowSet getNextRowSet(
      String operationHandleStr, String fetchOrientation, Integer maxRows) {
    String path = String.format("%s/%s/rowset", API_BASE_PATH, operationHandleStr);
    Map<String, Object> params = new HashMap<>();
    if (fetchOrientation != null) params.put("fetchorientation", fetchOrientation);
    if (maxRows != null) params.put("maxrows", maxRows);
    return this.getClient().get(path, params, ResultRowSet.class, client.getAuthHeader());
  }

  private IRestClient getClient() {
    return this.client.getHttpClient();
  }
}
