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

import static org.apache.kyuubi.client.RestClientTestUtils.*;

import java.io.IOException;
import java.util.Base64;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.kyuubi.client.api.v1.dto.Batch;
import org.apache.kyuubi.client.api.v1.dto.CloseBatchResponse;
import org.apache.kyuubi.client.api.v1.dto.GetBatchesResponse;
import org.apache.kyuubi.client.api.v1.dto.OperationLog;
import org.apache.kyuubi.client.api.v1.dto.ReassignBatchResponse;
import org.apache.kyuubi.client.util.JsonUtils;

public class BatchTestServlet extends HttpServlet {

  private static String authSchema = BASIC_AUTH;
  private static boolean allowAnonymous;

  public static void setAuthSchema(String schema) {
    authSchema = schema;
  }

  public static void allowAnonymous(boolean flag) {
    allowAnonymous = flag;
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    if (!validAuthHeader(req, resp)) return;

    if (req.getPathInfo().matches("/api/v1/batches/\\d+")) {
      resp.setStatus(HttpServletResponse.SC_OK);

      Batch batch = generateTestBatch();
      resp.getWriter().write(JsonUtils.toJson(batch));
      resp.getWriter().flush();
    } else if (req.getPathInfo().matches("/api/v1/batches")
        && req.getQueryString().matches("\\w+(=\\w*)(&\\w+(=\\w*))+$")) {
      resp.setStatus(HttpServletResponse.SC_OK);

      GetBatchesResponse batchesResponse = generateTestBatchesResponse();
      resp.getWriter().write(JsonUtils.toJson(batchesResponse));
      resp.getWriter().flush();
    } else if (req.getPathInfo().matches("/api/v1/batches/\\d+/localLog")) {
      resp.setStatus(HttpServletResponse.SC_OK);

      OperationLog log = generateTestOperationLog();
      resp.getWriter().write(JsonUtils.toJson(log));
      resp.getWriter().flush();
    } else {
      resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    if (!validAuthHeader(req, resp)) return;

    if (req.getPathInfo().equalsIgnoreCase("/api/v1/batches")) {
      resp.setStatus(HttpServletResponse.SC_OK);

      Batch batch = generateTestBatch();
      resp.getWriter().write(JsonUtils.toJson(batch));
      resp.getWriter().flush();
    } else if (req.getPathInfo().equalsIgnoreCase("/api/v1/batches/reassign")) {
      resp.setStatus(HttpServletResponse.SC_OK);

      resp.getWriter().write(JsonUtils.toJson(new ReassignBatchResponse()));
      resp.getWriter().flush();
    } else {
      resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
    }
  }

  @Override
  protected void doPut(HttpServletRequest req, HttpServletResponse resp) {
    resp.setStatus(HttpServletResponse.SC_OK);
  }

  @Override
  protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    if (!validAuthHeader(req, resp)) return;

    if (req.getPathInfo().matches("/api/v1/batches/\\d+")) {
      resp.setStatus(HttpServletResponse.SC_OK);

      CloseBatchResponse closeResp = generateTestCloseBatchResp();
      resp.getWriter().write(JsonUtils.toJson(closeResp));
      resp.getWriter().flush();
    } else {
      resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
    }
  }

  private boolean validAuthHeader(HttpServletRequest req, HttpServletResponse resp) {
    // check auth header existence
    String authHeader = req.getHeader("Authorization");
    if (allowAnonymous && authHeader == null) {
      return true;
    }
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

    // check basic username and password
    if (authSchema.equals(BASIC_AUTH)) {
      String authorization = authHeader.split(" ")[1].trim();
      String creds = new String(Base64.getDecoder().decode(authorization));
      if (allowAnonymous) {
        if (!creds.equals(TEST_USERNAME + ":")) {
          resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
          return false;
        }
      } else {
        if (!creds.equals(TEST_USERNAME + ":" + TEST_PASSWORD)) {
          resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
          return false;
        }
      }
    }

    return true;
  }
}
