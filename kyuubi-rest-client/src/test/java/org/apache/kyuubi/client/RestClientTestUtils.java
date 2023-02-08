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
import java.util.Collections;
import java.util.List;
import org.apache.kyuubi.client.api.v1.dto.Batch;
import org.apache.kyuubi.client.api.v1.dto.BatchRequest;
import org.apache.kyuubi.client.api.v1.dto.CloseBatchResponse;
import org.apache.kyuubi.client.api.v1.dto.GetBatchesResponse;
import org.apache.kyuubi.client.api.v1.dto.OperationLog;

public class RestClientTestUtils {

  public static final String BASIC_AUTH = "BASIC";
  public static final String NEGOTIATE_AUTH = "NEGOTIATE";

  public static final String TEST_USERNAME = "test_user";
  public static final String TEST_PASSWORD = "test_password";

  public static final Long BATCH_CREATE_TIME = System.currentTimeMillis();

  public static Batch generateTestBatch() {
    return generateTestBatch("71535");
  }

  public static CloseBatchResponse generateTestCloseBatchResp() {
    return new CloseBatchResponse(true, "");
  }

  public static Batch generateTestBatch(String id) {
    Batch batch =
        new Batch(
            id,
            TEST_USERNAME,
            "spark",
            "batch_name",
            0,
            id,
            null,
            "RUNNING",
            null,
            "192.168.31.130:64573",
            "RUNNING",
            BATCH_CREATE_TIME,
            0);

    return batch;
  }

  public static BatchRequest generateTestBatchRequest() {
    BatchRequest batchRequest =
        new BatchRequest(
            "spark",
            "/MySpace/kyuubi-spark-sql-engine_2.12-1.6.0-SNAPSHOT.jar",
            "org.apache.kyuubi.engine.spark.SparkSQLEngine",
            "test_batch",
            Collections.singletonMap("spark.driver.memory", "16m"),
            Collections.emptyList());
    return batchRequest;
  }

  public static GetBatchesResponse generateTestBatchesResponse() {
    Batch b1 = generateTestBatch("1");
    Batch b2 = generateTestBatch("2");
    List<Batch> batches = Arrays.asList(b1, b2);

    return new GetBatchesResponse(0, 2, batches);
  }

  public static OperationLog generateTestOperationLog() {
    List<String> logs =
        Arrays.asList(
            "13:15:13.523 INFO org.apache.curator.framework.state."
                + "ConnectionStateManager: State change: CONNECTED",
            "13:15:13.528 INFO org.apache.kyuubi." + "engine.EngineRef: Launching engine:");
    return new OperationLog(logs, 2);
  }
}
