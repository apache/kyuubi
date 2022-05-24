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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.kyuubi.client.api.v1.dto.Batch;
import org.apache.kyuubi.client.api.v1.dto.BatchRequest;
import org.apache.kyuubi.client.api.v1.dto.GetBatchesResponse;
import org.apache.kyuubi.client.api.v1.dto.OperationLog;
import org.apache.kyuubi.client.exception.KyuubiRestException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BatchRestClientTest {

  private BatchRestApi spnegoBatchRestApi;
  private BatchRestApi basicBatchRestApi;

  private KerberizedTestHelper kerberizedTestHelper;
  private ServerTestHelper serverTestHelper;

  private static final String BASIC_AUTH = "BASIC";
  private static final String NEGOTIATE_AUTH = "NEGOTIATE";

  @Before
  public void setUp() throws Exception {
    kerberizedTestHelper = new KerberizedTestHelper();
    serverTestHelper = new ServerTestHelper();

    kerberizedTestHelper.setup();
    serverTestHelper.setup(TestServlet.class);

    kerberizedTestHelper.login();
    KyuubiRestClient spnegoClient =
        new KyuubiRestClient.Builder("https://localhost:8443", "batch")
            .authSchema(KyuubiRestClient.AuthSchema.SPNEGO)
            .build();
    spnegoBatchRestApi = new BatchRestApi(spnegoClient);

    KyuubiRestClient basicClient =
        new KyuubiRestClient.Builder("https://localhost:8443", "batch")
            .authSchema(KyuubiRestClient.AuthSchema.BASIC)
            .username("test")
            .password("test")
            .build();
    basicBatchRestApi = new BatchRestApi(basicClient);
  }

  @After
  public void tearDown() throws Exception {
    kerberizedTestHelper.stop();
    serverTestHelper.stop();
  }

  public static class TestServlet extends HttpServlet {

    private static String authSchema = BASIC_AUTH;

    public static void setAuthSchema(String schema) {
      authSchema = schema;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      if (!validAuthHeader(req, resp)) return;

      if (req.getPathInfo().matches("/api/v1/batch/\\d+")) {
        resp.setStatus(HttpServletResponse.SC_OK);

        Batch batch = generateTestBatch();
        resp.getWriter().write(new ObjectMapper().writeValueAsString(batch));
        resp.getWriter().flush();
      } else if (req.getPathInfo().matches("/api/v1/batch")
          && req.getQueryString().matches("[\\w]+(=[\\w]*)(&[\\w]+(=[\\w]*))+$")) {
        resp.setStatus(HttpServletResponse.SC_OK);

        GetBatchesResponse batchesResponse = generateTestBatchesResponse();
        resp.getWriter().write(new ObjectMapper().writeValueAsString(batchesResponse));
        resp.getWriter().flush();
      } else if (req.getPathInfo().matches("/api/v1/batch/\\d+/localLog")) {
        resp.setStatus(HttpServletResponse.SC_OK);

        OperationLog log = generateTestOperationLog();
        resp.getWriter().write(new ObjectMapper().writeValueAsString(log));
        resp.getWriter().flush();
      } else {
        resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
      }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      if (!validAuthHeader(req, resp)) return;

      if (req.getPathInfo().equalsIgnoreCase("/api/v1/batch")) {
        resp.setStatus(HttpServletResponse.SC_OK);

        Batch batch = generateTestBatch();
        resp.getWriter().write(new ObjectMapper().writeValueAsString(batch));
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
      if (!validAuthHeader(req, resp)) return;

      if (req.getPathInfo().matches("/api/v1/batch/\\d+")
          && req.getQueryString().matches("[\\w.]+(=[\\w]*)(&[\\w.]+(=[\\w]*))+$")) {
        resp.setStatus(HttpServletResponse.SC_OK);
      } else {
        resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
      }
    }

    private boolean validAuthHeader(HttpServletRequest req, HttpServletResponse resp) {
      // check auth header existence
      String authHeader = req.getHeader("Authorization");
      if (authHeader == null) {
        resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        return false;
      }

      // check auth schema
      String schema = authHeader.split(" ")[0].trim();
      if (!schema.equals(authSchema)) {
        resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        return false;
      }

      return true;
    }
  }

  @Test
  public void createBatchTest() throws IOException, KyuubiRestException {
    // test spnego auth
    TestServlet.setAuthSchema(NEGOTIATE_AUTH);

    BatchRequest batchRequest =
        new BatchRequest(
            "spark",
            "/MySpace/kyuubi-spark-sql-engine_2.12-1.6.0-SNAPSHOT.jar",
            "org.apache.kyuubi.engine.spark.SparkSQLEngine",
            "test_batch");
    Batch expectedBatch = generateTestBatch();
    Batch result = spnegoBatchRestApi.createBatch(batchRequest);

    assertEquals(result.getId(), expectedBatch.getId());
    assertEquals(result.getBatchType(), expectedBatch.getBatchType());
    assertEquals(result.getState(), expectedBatch.getState());

    // test basic auth
    TestServlet.setAuthSchema(BASIC_AUTH);
    result = basicBatchRestApi.createBatch(batchRequest);

    assertEquals(result.getId(), expectedBatch.getId());
    assertEquals(result.getBatchType(), expectedBatch.getBatchType());
    assertEquals(result.getState(), expectedBatch.getState());
  }

  @Test
  public void getBatchByIdTest() throws IOException, KyuubiRestException {
    // test spnego auth
    TestServlet.setAuthSchema(NEGOTIATE_AUTH);

    Batch expectedBatch = generateTestBatch();
    Batch result = spnegoBatchRestApi.getBatchById("71535");

    assertEquals(result.getId(), expectedBatch.getId());
    assertEquals(result.getBatchType(), expectedBatch.getBatchType());
    assertEquals(result.getState(), expectedBatch.getState());

    // test basic auth
    TestServlet.setAuthSchema(BASIC_AUTH);

    result = basicBatchRestApi.getBatchById("71535");

    assertEquals(result.getId(), expectedBatch.getId());
    assertEquals(result.getBatchType(), expectedBatch.getBatchType());
    assertEquals(result.getState(), expectedBatch.getState());
  }

  @Test
  public void getBatchInfoListTest() throws IOException, KyuubiRestException {
    // test spnego auth
    TestServlet.setAuthSchema(NEGOTIATE_AUTH);

    GetBatchesResponse expectedBatchesInfo = generateTestBatchesResponse();
    GetBatchesResponse result = spnegoBatchRestApi.getBatchInfoList("spark", 0, 10);

    assertEquals(expectedBatchesInfo.getBatches().size(), result.getBatches().size());
    assertEquals(expectedBatchesInfo.getFrom(), result.getFrom());
    assertEquals(expectedBatchesInfo.getTotal(), result.getTotal());

    // test basic auth
    TestServlet.setAuthSchema(BASIC_AUTH);

    result = basicBatchRestApi.getBatchInfoList("spark", 0, 10);

    assertEquals(expectedBatchesInfo.getBatches().size(), result.getBatches().size());
    assertEquals(expectedBatchesInfo.getFrom(), result.getFrom());
    assertEquals(expectedBatchesInfo.getTotal(), result.getTotal());
  }

  @Test
  public void getOperationLogTest() throws IOException, KyuubiRestException {
    // test spnego auth
    TestServlet.setAuthSchema(NEGOTIATE_AUTH);

    OperationLog expectedOperationLog = generateTestOperationLog();
    OperationLog result = spnegoBatchRestApi.getOperationLog("71535", 0, 2);

    assertEquals(expectedOperationLog.getRowCount(), result.getRowCount());

    // test basic auth
    TestServlet.setAuthSchema(BASIC_AUTH);

    result = basicBatchRestApi.getOperationLog("71535", 0, 2);

    assertEquals(expectedOperationLog.getRowCount(), result.getRowCount());
  }

  @Test
  public void deleteBatchTest() throws IOException, KyuubiRestException {
    // test spnego auth
    TestServlet.setAuthSchema(NEGOTIATE_AUTH);
    spnegoBatchRestApi.deleteBatch("71535", true, "b_test");

    // test basic auth
    TestServlet.setAuthSchema(BASIC_AUTH);
    basicBatchRestApi.deleteBatch("71535", true, "b_test");
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
