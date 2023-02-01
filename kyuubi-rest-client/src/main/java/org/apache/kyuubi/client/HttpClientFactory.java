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

import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.BasicHttpClientConnectionManager;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.config.Registry;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.ssl.TrustStrategy;
import org.apache.kyuubi.client.exception.KyuubiRestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HttpClientFactory.class);

  public static CloseableHttpClient createHttpClient(RestClientConf conf) {
    RequestConfig requestConfig =
        RequestConfig.custom()
            .setResponseTimeout(conf.getSocketTimeout(), TimeUnit.MILLISECONDS)
            .build();
    Registry<ConnectionSocketFactory> socketFactoryRegistry;
    SSLConnectionSocketFactory sslSocketFactory;
    try {
      TrustStrategy acceptingTrustStrategy = (cert, authType) -> true;
      SSLContext sslContext =
          SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
      sslSocketFactory = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
      socketFactoryRegistry =
          RegistryBuilder.<ConnectionSocketFactory>create()
              .register("https", sslSocketFactory)
              .register("http", new PlainConnectionSocketFactory())
              .build();
    } catch (Exception e) {
      LOG.error("Error: ", e);
      throw new KyuubiRestException("Failed to create HttpClient", e);
    }
    BasicHttpClientConnectionManager cm =
        new BasicHttpClientConnectionManager(socketFactoryRegistry);
    ConnectionConfig connConfig =
        ConnectionConfig.custom()
            .setConnectTimeout(conf.getConnectTimeout(), TimeUnit.MILLISECONDS)
            .build();
    cm.setConnectionConfig(connConfig);

    return HttpClientBuilder.create()
        .setDefaultRequestConfig(requestConfig)
        .setConnectionManager(cm)
        .build();
  }
}
