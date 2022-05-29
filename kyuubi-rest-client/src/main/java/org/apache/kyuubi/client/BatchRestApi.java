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
import org.apache.kyuubi.client.api.v1.dto.Batch;
import org.apache.kyuubi.client.api.v1.dto.BatchRequest;
import org.apache.kyuubi.client.api.v1.dto.GetBatchesResponse;
import org.apache.kyuubi.client.api.v1.dto.OperationLog;
import org.apache.kyuubi.client.exception.KyuubiRestException;
import org.apache.kyuubi.client.util.JsonUtil;

public class BatchRestApi {

  private KyuubiRestClient client;

  private static final String API_BASE_PATH = "batches";

  public BatchRestApi(KyuubiRestClient client) {
    this.client = client;
  }

  public Batch createBatch(BatchRequest request) throws KyuubiRestException {
    String requestBody = JsonUtil.toJson(request);
    return this.getClient().post(API_BASE_PATH, requestBody, Batch.class);
  }

  public Batch getBatchById(String batchId) throws KyuubiRestException {
    String path = String.format("%s/%s", API_BASE_PATH, batchId);
    return this.getClient().get(path, null, Batch.class);
  }

  public GetBatchesResponse listBatches(String batchType, int from, int size)
      throws KyuubiRestException {
    Map<String, Object> params = new HashMap<>();
    params.put("batchType", batchType);
    params.put("from", from);
    params.put("size", size);
    return this.getClient().get(API_BASE_PATH, params, GetBatchesResponse.class);
  }

  public OperationLog getBatchLocalLog(String batchId, int from, int size)
      throws KyuubiRestException {
    Map<String, Object> params = new HashMap<>();
    params.put("batchId", batchId);
    params.put("from", from);
    params.put("size", size);

    String path = String.format("%s/%s/localLog", API_BASE_PATH, batchId);
    return this.getClient().get(path, params, OperationLog.class);
  }

  public void deleteBatch(String batchId, boolean killApp, String hs2ProxyUser)
      throws KyuubiRestException {
    Map<String, Object> params = new HashMap<>();
    params.put("killApp", killApp);
    params.put("hive.server2.proxy.user", hs2ProxyUser);

    String path = String.format("%s/%s", API_BASE_PATH, batchId);
    this.getClient().delete(path, params);
  }

  private RestClient getClient() {
    return this.client.getHttpClient();
  }
}
