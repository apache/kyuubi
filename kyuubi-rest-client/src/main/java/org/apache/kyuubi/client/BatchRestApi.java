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

import java.io.File;
import java.nio.file.Paths;
import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.client.api.v1.dto.*;
import org.apache.kyuubi.client.util.JsonUtils;
import org.apache.kyuubi.client.util.VersionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchRestApi {
  static final Logger LOG = LoggerFactory.getLogger(BatchRestApi.class);

  private KyuubiRestClient client;

  private static final String API_BASE_PATH = "batches";

  private BatchRestApi() {}

  public BatchRestApi(KyuubiRestClient client) {
    this.client = client;
  }

  public Batch createBatch(BatchRequest request) {
    setClientVersion(request);
    String requestBody = JsonUtils.toJson(request);
    return this.getClient().post(API_BASE_PATH, requestBody, Batch.class, client.getAuthHeader());
  }

  public Batch createBatch(BatchRequest request, File resourceFile) {
    return createBatch(request, resourceFile, Collections.emptyList());
  }

  public Batch createBatch(BatchRequest request, File resourceFile, List<String> extraResources) {
    setClientVersion(request);
    Map<String, MultiPart> multiPartMap = new HashMap<>();
    multiPartMap.put("batchRequest", new MultiPart(MultiPart.MultiPartType.JSON, request));
    multiPartMap.put("resourceFile", new MultiPart(MultiPart.MultiPartType.FILE, resourceFile));
    extraResources.stream()
        .distinct()
        .filter(StringUtils::isNotBlank)
        .map(
            path -> {
              File file = Paths.get(path).toFile();
              if (!file.exists()) {
                throw new RuntimeException("File not existed, path: " + path);
              }
              return file;
            })
        .forEach(
            file ->
                multiPartMap.put(
                    file.getName(), new MultiPart(MultiPart.MultiPartType.FILE, file)));
    return this.getClient().post(API_BASE_PATH, multiPartMap, Batch.class, client.getAuthHeader());
  }

  public Batch getBatchById(String batchId) {
    return getBatchById(batchId, Collections.emptyMap());
  }

  public Batch getBatchById(String batchId, Map<String, String> headers) {
    String path = String.format("%s/%s", API_BASE_PATH, batchId);
    return this.getClient().get(path, null, Batch.class, client.getAuthHeader(), headers);
  }

  public GetBatchesResponse listBatches(
      String batchType,
      String batchUser,
      String batchState,
      Long createTime,
      Long endTime,
      int from,
      int size) {
    return listBatches(batchType, batchUser, batchState, null, createTime, endTime, from, size);
  }

  public GetBatchesResponse listBatches(
      String batchType,
      String batchUser,
      String batchState,
      String batchName,
      Long createTime,
      Long endTime,
      int from,
      int size) {
    return listBatches(
        batchType, batchUser, batchState, batchName, createTime, endTime, from, size, false);
  }

  public GetBatchesResponse listBatches(
      String batchType,
      String batchUser,
      String batchState,
      String batchName,
      Long createTime,
      Long endTime,
      int from,
      int size,
      boolean desc) {
    Map<String, Object> params = new HashMap<>();
    params.put("batchType", batchType);
    params.put("batchUser", batchUser);
    params.put("batchState", batchState);
    params.put("batchName", batchName);
    if (null != createTime && createTime > 0) {
      params.put("createTime", createTime);
    }
    if (null != endTime && endTime > 0) {
      params.put("endTime", endTime);
    }
    params.put("from", from);
    params.put("size", size);
    params.put("desc", desc);
    return this.getClient()
        .get(API_BASE_PATH, params, GetBatchesResponse.class, client.getAuthHeader());
  }

  public OperationLog getBatchLocalLog(String batchId, int from, int size) {
    return getBatchLocalLog(batchId, from, size, Collections.emptyMap());
  }

  public OperationLog getBatchLocalLog(
      String batchId, int from, int size, Map<String, String> headers) {
    Map<String, Object> params = new HashMap<>();
    params.put("from", from);
    params.put("size", size);

    String path = String.format("%s/%s/localLog", API_BASE_PATH, batchId);
    return this.getClient().get(path, params, OperationLog.class, client.getAuthHeader(), headers);
  }

  /**
   * hs2ProxyUser for delete batch is deprecated since 1.8.1, please use {@link
   * #deleteBatch(String)} instead.
   */
  @Deprecated
  public CloseBatchResponse deleteBatch(String batchId, String hs2ProxyUser) {
    LOG.warn(
        "The method `deleteBatch(batchId, hs2ProxyUser)` is deprecated since 1.8.1, "
            + "using `deleteBatch(batchId)` instead.");
    Map<String, Object> params = new HashMap<>();
    params.put("hive.server2.proxy.user", hs2ProxyUser);

    String path = String.format("%s/%s", API_BASE_PATH, batchId);
    return this.getClient().delete(path, params, CloseBatchResponse.class, client.getAuthHeader());
  }

  public CloseBatchResponse deleteBatch(String batchId) {
    return deleteBatch(batchId, Collections.emptyMap());
  }

  public CloseBatchResponse deleteBatch(String batchId, Map<String, String> headers) {
    String path = String.format("%s/%s", API_BASE_PATH, batchId);
    return this.getClient()
        .delete(path, null, CloseBatchResponse.class, client.getAuthHeader(), headers);
  }

  private IRestClient getClient() {
    return this.client.getHttpClient();
  }

  private void setClientVersion(BatchRequest request) {
    if (request != null) {
      Map<String, String> newConf = new HashMap<>(request.getConf());
      newConf.put(VersionUtils.KYUUBI_CLIENT_VERSION_KEY, VersionUtils.getVersion());
      request.setConf(newConf);
    }
  }
}
