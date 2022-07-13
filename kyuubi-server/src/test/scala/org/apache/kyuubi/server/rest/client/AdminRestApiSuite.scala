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

package org.apache.kyuubi.server.rest.client

import org.apache.kyuubi.RestClientTestHelper
import org.apache.kyuubi.client.{AdminRestApi, KyuubiRestClient}

class AdminRestApiSuite extends RestClientTestHelper {
  test("refresh kyuubi server hadoop conf") {
    val spnegoKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.SPNEGO)
        .spnegoHost("localhost")
        .build()
    val adminRestApi = new AdminRestApi(spnegoKyuubiRestClient)
    val result = adminRestApi.refreshHadoopConf()
    assert(result === s"Refresh the hadoop conf for ${fe.connectionUrl} successfully")
  }
}
