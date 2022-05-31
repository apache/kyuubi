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

import java.net.URI;
import java.util.Base64;
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.kyuubi.client.util.AuthUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KyuubiRestClient implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(KyuubiRestClient.class);

  private RestClient httpClient;

  private AuthSchema authSchema;
  private String spnegoHost;
  private String basicAuthHeader = "";

  /** Specifies the version of the Kyuubi API to communicate with. */
  public enum ApiVersion {
    V1;

    public String getApiNamespace() {
      return ("api/" + name().toLowerCase());
    }
  }

  public enum AuthSchema {
    BASIC,
    SPNEGO
  }

  @Override
  public void close() throws Exception {
    if (httpClient != null) {
      httpClient.close();
    }
  }

  private KyuubiRestClient() {}

  private KyuubiRestClient(Builder builder) {
    // Remove the trailing "/" from the hostUrl if present
    String hostUrl = builder.hostUrl.replaceAll("/$", "");
    String baseUrl = String.format("%s/%s", hostUrl, builder.version.getApiNamespace());

    CloseableHttpClient httpclient = initHttpClient(builder);

    this.httpClient = new RestClient(baseUrl, httpclient);

    this.authSchema = builder.authSchema;
    this.spnegoHost = builder.spnegoHost;
    if (this.authSchema == AuthSchema.BASIC && StringUtils.isNotBlank(builder.username)) {
      String authorization = String.format("%s:%s", builder.username, builder.password);
      this.basicAuthHeader =
          String.format("BASIC %s", Base64.getEncoder().encodeToString(authorization.getBytes()));
    }
  }

  public String getAuthHeader() {
    String header = "";
    switch (authSchema) {
      case BASIC:
        header = this.basicAuthHeader;
        break;
      case SPNEGO:
        try {
          header = String.format("NEGOTIATE %s", AuthUtil.generateToken(spnegoHost));
        } catch (Exception e) {
          LOG.error("Error: ", e);
          throw new RuntimeException("Failed to generate spnego auth header for " + spnegoHost);
        }
        break;
      default:
        throw new RuntimeException("Unsupported auth schema");
    }
    return header;
  }

  public RestClient getHttpClient() {
    return httpClient;
  }

  private CloseableHttpClient initHttpClient(Builder builder) {
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
      LOG.error("Error: ", e);
      throw new RuntimeException(e);
    }

    CloseableHttpClient httpclient =
        HttpClientBuilder.create()
            .setDefaultRequestConfig(requestConfig)
            .setSSLSocketFactory(sslSocketFactory)
            .build();
    return httpclient;
  }

  public static Builder builder(String hostUrl) {
    return new Builder(hostUrl);
  }

  public static class Builder {

    private String hostUrl;

    private String spnegoHost;

    private ApiVersion version = ApiVersion.V1;

    private AuthSchema authSchema = AuthSchema.BASIC;

    private String username;

    private String password;

    private int socketTimeout = 3000;

    private int connectTimeout = 3000;

    public Builder(String hostUrl) {
      this.hostUrl = hostUrl;
    }

    public Builder spnegoHost(String host) {
      this.spnegoHost = host;
      return this;
    }

    public Builder apiVersion(ApiVersion version) {
      this.version = version;
      return this;
    }

    public Builder authSchema(AuthSchema authSchema) {
      this.authSchema = authSchema;
      return this;
    }

    public Builder username(String username) {
      this.username = username;
      return this;
    }

    public Builder password(String password) {
      this.password = password;
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
      if (StringUtils.isBlank(hostUrl)) {
        throw new IllegalArgumentException("hostUrl cannot be blank.");
      }

      if (authSchema == AuthSchema.SPNEGO && StringUtils.isBlank(spnegoHost)) {
        try {
          this.spnegoHost = new URI(hostUrl).getHost();
        } catch (Exception e) {
          throw new IllegalArgumentException("spnegoHost is invalid.");
        }
      }

      this.password = StringUtils.isNotBlank(password) ? password : "";

      return new KyuubiRestClient(this);
    }
  }
}
