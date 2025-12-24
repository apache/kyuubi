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

import java.util.Collections;
import java.util.Map;
import org.apache.kyuubi.client.api.v1.dto.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseRestApi {
  private static final Logger LOG = LoggerFactory.getLogger(BaseRestApi.class);
  private KyuubiRestClient client;

  private BaseRestApi() {}

  public BaseRestApi(KyuubiRestClient client) {
    this.client = client;
  }

  public String ping() {
    return ping(Collections.emptyMap());
  }

  public String ping(Map<String, String> headers) {
    return this.getClient().get("ping", null, client.getAuthHeader(), headers);
  }

  public VersionInfo getVersionInfo() {
    return this.getClient().get("version", null, VersionInfo.class, client.getAuthHeader());
  }

  private IRestClient getClient() {
    return this.client.getHttpClient();
  }
}
