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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.apache.kyuubi.client.api.v1.dto.Batch;
import org.apache.kyuubi.client.api.v1.dto.BatchRequest;
import org.apache.kyuubi.client.api.v1.dto.GetBatchesResponse;
import org.apache.kyuubi.client.api.v1.dto.OperationLog;
import org.apache.kyuubi.client.exception.KyuubiRestException;

public class BatchRestApi {

  private KyuubiRestClient client;

  public BatchRestApi(KyuubiRestClient client) {
    this.client = client;
  }

  public RestClient getClient() {
    return this.client.getHttpClient();
  }

  public Batch createBatch(BatchRequest request) throws KyuubiRestException {
    String jsonBody = null;
    try {
      jsonBody = new ObjectMapper().writeValueAsString(request);
    } catch (JsonProcessingException e) {
      throw new KyuubiRestException("cannot convert batch request body to json", e);
    }

    return this.getClient().post(jsonBody, new TypeReference<Batch>() {});
  }

  public Batch getBatchById(String batchId) throws KyuubiRestException {
    return this.getClient().get(batchId, null, new TypeReference<Batch>() {});
  }

  public GetBatchesResponse getBatchInfoList(String batchType, int from, int size)
      throws KyuubiRestException {
    Map<String, Object> params = new HashMap<>();
    params.put("batchType", batchType);
    params.put("from", from);
    params.put("size", size);
    return this.getClient().get(null, params, new TypeReference<GetBatchesResponse>() {});
  }

  public OperationLog getOperationLog(String batchId, int from, int size)
      throws KyuubiRestException {
    Map<String, Object> params = new HashMap<>();
    params.put("batchId", batchId);
    params.put("from", from);
    params.put("size", size);
    return this.getClient()
        .get(batchId + "/localLog", params, new TypeReference<OperationLog>() {});
  }

  public void deleteBatch(String batchId, boolean killApp, String hs2ProxyUser)
      throws KyuubiRestException {
    Map<String, Object> params = new HashMap<>();
    params.put("killApp", killApp);
    params.put("hive.server2.proxy.user", hs2ProxyUser);
    this.getClient().delete(batchId, params);
  }
}
