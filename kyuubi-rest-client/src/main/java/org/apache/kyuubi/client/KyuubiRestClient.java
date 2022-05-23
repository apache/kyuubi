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

public class KyuubiRestClient {

  private RestClient httpClient;

  /** Specifies the version of the Kyuubi API to communicate with. */
  public enum ApiVersion {
    V1;

    public String getApiNamespace() {
      return ("api/" + name().toLowerCase());
    }
  }

  public KyuubiRestClient(Builder builder) {
    // Remove the trailing "/" from the hostUrl if present
    String hostUrl = builder.hostUrl.replaceAll("/$", "");
    String baseUrl =
        String.format("%s/%s/%s", hostUrl, builder.version.getApiNamespace(), builder.apiBasePath);

    RequestConfig requestConfig =
        RequestConfig.custom()
            .setSocketTimeout(builder.socketTimeout)
            .setConnectTimeout(builder.connectTimeout)
            .build();
    SSLConnectionSocketFactory sslSocketFactory;
    try {
      TrustStrategy acceptingTrustStrategy = (cert, authType) -> true;
      SSLContext sslContext =
          SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
      sslSocketFactory = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    CloseableHttpClient httpclient =
        HttpClientBuilder.create()
            .setDefaultRequestConfig(requestConfig)
            .setSSLSocketFactory(sslSocketFactory)
            .build();

    this.httpClient = new RestClient(baseUrl, httpclient);
  }

  public RestClient getHttpClient() {
    return httpClient;
  }

  public static class Builder {

    private String hostUrl;

    private String apiBasePath;

    private ApiVersion version = ApiVersion.V1;

    private int socketTimeout = 3000;

    private int connectTimeout = 3000;

    public Builder(String hostUrl, String apiBasePath) {
      this.hostUrl = hostUrl;
      this.apiBasePath = apiBasePath;
    }

    public Builder apiVersion(ApiVersion version) {
      this.version = version;
      return this;
    }

    public Builder socketTimeout(int socketTimeout) {
      this.socketTimeout = socketTimeout;
      return this;
    }

    public Builder connectionTimeout(int connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public KyuubiRestClient build() {
      return new KyuubiRestClient(this);
    }
  }
}
