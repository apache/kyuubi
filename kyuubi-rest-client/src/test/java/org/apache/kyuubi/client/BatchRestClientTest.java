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

import static org.junit.Assert.assertEquals;

import com.google.gson.Gson;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.kyuubi.KyuubiException;
import org.apache.kyuubi.client.api.v1.dto.Batch;
import org.apache.kyuubi.client.api.v1.dto.BatchRequest;
import org.apache.kyuubi.client.api.v1.dto.GetBatchesResponse;
import org.apache.kyuubi.client.api.v1.dto.OperationLog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BatchRestClientTest {

  private BatchRestApi batchRestApi;

  private KerberizedTestHelper kerberizedTestHelper;
  private ServerTestHelper serverTestHelper;

  @Before
  public void setUp() throws Exception {
    kerberizedTestHelper = new KerberizedTestHelper();
    serverTestHelper = new ServerTestHelper();

    kerberizedTestHelper.setup();
    serverTestHelper.setup(TestServlet.class);

    KyuubiRestClient client =
        new KyuubiRestClient.Builder("https://localhost:8443", "batch").build();
    this.batchRestApi = new BatchRestApi(client);
  }

  @After
  public void tearDown() throws Exception {
    kerberizedTestHelper.stop();
    serverTestHelper.stop();
  }

  public static class TestServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      if (checkAuthHeader(req, resp)) return;

      if (req.getPathInfo().matches("/api/v1/batch/\\d+")) {
        resp.setStatus(HttpServletResponse.SC_OK);

        Batch batch = generateTestBatch();
        resp.getWriter().write(new Gson().toJson(batch));
        resp.getWriter().flush();
      } else if (req.getPathInfo().matches("/api/v1/batch")
          && req.getQueryString().matches("[\\w]+(=[\\w]*)(&[\\w]+(=[\\w]*))+$")) {
        resp.setStatus(HttpServletResponse.SC_OK);

        GetBatchesResponse batchesResponse = generateTestBatchesResponse();
        resp.getWriter().write(new Gson().toJson(batchesResponse));
        resp.getWriter().flush();
      } else if (req.getPathInfo().matches("/api/v1/batch/\\d+/locallog")) {
        resp.setStatus(HttpServletResponse.SC_OK);

        OperationLog log = generateTestOperationLog();
        resp.getWriter().write(new Gson().toJson(log));
        resp.getWriter().flush();
      } else {
        resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
      }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      if (checkAuthHeader(req, resp)) return;

      if (req.getPathInfo().equalsIgnoreCase("/api/v1/batch")) {
        resp.setStatus(HttpServletResponse.SC_OK);

        Batch batch = generateTestBatch();
        resp.getWriter().write(new Gson().toJson(batch));
        resp.getWriter().flush();
      } else {
        resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
      }
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      resp.setStatus(HttpServletResponse.SC_OK);
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      if (checkAuthHeader(req, resp)) return;

      if (req.getPathInfo().matches("/api/v1/batch/\\d+")
          && req.getQueryString().matches("[\\w.]+(=[\\w]*)(&[\\w.]+(=[\\w]*))+$")) {
        resp.setStatus(HttpServletResponse.SC_OK);
      } else {
        resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
      }
    }

    private boolean checkAuthHeader(HttpServletRequest req, HttpServletResponse resp) {
      if (req.getHeader("Authorization") == null) {
        resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        return true;
      }
      return false;
    }
  }

  @Test
  public void createBatchTest() throws IOException, KyuubiException {
    kerberizedTestHelper.login();

    BatchRequest batchRequest =
        new BatchRequest(
            "spark",
            "/MySpace/kyuubi-spark-sql-engine_2.12-1.6.0-SNAPSHOT.jar",
            "org.apache.kyuubi.engine.spark.SparkSQLEngine",
            "test_batch");
    Batch expectedBatch = generateTestBatch();
    Batch result = batchRestApi.createBatch(batchRequest);

    assertEquals(result.getId(), expectedBatch.getId());
    assertEquals(result.getBatchType(), expectedBatch.getBatchType());
    assertEquals(result.getState(), expectedBatch.getState());
  }

  @Test
  public void getBatchByIdTest() throws IOException, KyuubiException {
    kerberizedTestHelper.login();

    Batch expectedBatch = generateTestBatch();
    Batch result = batchRestApi.getBatchById("71535");

    assertEquals(result.getId(), expectedBatch.getId());
    assertEquals(result.getBatchType(), expectedBatch.getBatchType());
    assertEquals(result.getState(), expectedBatch.getState());
  }

  @Test
  public void getBatchInfoListTest() throws IOException, KyuubiException {
    kerberizedTestHelper.login();

    GetBatchesResponse expectedBatchesInfo = generateTestBatchesResponse();
    GetBatchesResponse result = batchRestApi.getBatchInfoList("spark", 0, 10);

    assertEquals(expectedBatchesInfo.getBatches().size(), result.getBatches().size());
    assertEquals(expectedBatchesInfo.getFrom(), result.getFrom());
    assertEquals(expectedBatchesInfo.getTotal(), result.getTotal());
  }

  @Test
  public void getOperationLogTest() throws IOException, KyuubiException {
    kerberizedTestHelper.login();

    OperationLog expectedOperationLog = generateTestOperationLog();
    OperationLog result = batchRestApi.getOperationLog("71535", 0, 2);

    assertEquals(expectedOperationLog.getRowCount(), result.getRowCount());
  }

  @Test
  public void deleteBatchTest() throws IOException, KyuubiException {
    kerberizedTestHelper.login();
    batchRestApi.deleteBatch("71535", true, "b_test");
  }

  private static Batch generateTestBatch() {
    return generateTestBatch("71535");
  }

  private static Batch generateTestBatch(String id) {
    Map<String, String> batchInfo = new HashMap<>();
    batchInfo.put("id", id);
    batchInfo.put(
        "name",
        "org.apache.spark.deploy.SparkSubmit --conf spark.master=local "
            + "--conf spark.kyuubi.session.engine.spark.max.lifetime=5000 "
            + "--conf spark.kyuubi.session.engine.check.interval=1000 "
            + "--conf spark.yarn.tags=f3dfa392-f55f-493c-bea8-559be4ba67e8 "
            + "--conf spark.app.name=spark-batch-submission "
            + "--class org.apache.kyuubi.engine.spark.SparkSQLEngine "
            + "--proxy-user anonymous "
            + "/MySpace/kyuubi-spark-sql-engine_2.12-1.6.0-SNAPSHOT.jar");
    batchInfo.put("state", "RUNNING");

    Batch batch = new Batch(id, "spark", batchInfo, "192.168.31.130:64573", "RUNNING");

    return batch;
  }

  private static GetBatchesResponse generateTestBatchesResponse() {
    Batch b1 = generateTestBatch("1");
    Batch b2 = generateTestBatch("2");
    List<Batch> batches = Arrays.asList(b1, b2);

    return new GetBatchesResponse(0, 2, batches);
  }

  private static OperationLog generateTestOperationLog() {
    List<String> logs =
        Arrays.asList(
            "13:15:13.523 INFO org.apache.curator.framework.state."
                + "ConnectionStateManager: State change: CONNECTED",
            "13:15:13.528 INFO org.apache.kyuubi." + "engine.EngineRef: Launching engine:");
    return new OperationLog(logs, 2);
  }
}
