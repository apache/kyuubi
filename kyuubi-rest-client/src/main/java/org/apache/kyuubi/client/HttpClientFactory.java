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

import javax.net.ssl.SSLContext;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.kyuubi.client.exception.KyuubiRestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HttpClientFactory.class);

  public static CloseableHttpClient createHttpClient(RestClientConf conf) {
    RequestConfig requestConfig =
        RequestConfig.custom()
            .setSocketTimeout(conf.getSocketTimeout())
            .setConnectTimeout(conf.getConnectTimeout())
            .build();
    SSLConnectionSocketFactory sslSocketFactory;
    try {
      TrustStrategy acceptingTrustStrategy = (cert, authType) -> true;
      SSLContext sslContext =
          SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
      sslSocketFactory = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
    } catch (Exception e) {
      LOG.error("Error: ", e);
      throw new KyuubiRestException("Failed to create HttpClient", e);
    }

    return HttpClientBuilder.create()
        .setDefaultRequestConfig(requestConfig)
        .setSSLSocketFactory(sslSocketFactory)
        .build();
  }
}
