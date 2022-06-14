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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.client.auth.*;

public class KyuubiRestClient implements AutoCloseable {

  private IRestClient httpClient;

  private AuthHeaderGenerator authHeaderGenerator;

  /** Specifies the version of the Kyuubi API to communicate with. */
  public enum ApiVersion {
    V1;

    public String getApiNamespace() {
      return ("api/" + name().toLowerCase());
    }
  }

  public enum AuthHeaderMethod {
    BASIC,
    SPNEGO,
    CUSTOM
  }

  @Override
  public void close() throws Exception {
    if (httpClient != null) {
      httpClient.close();
    }
  }

  private KyuubiRestClient() {}

  private KyuubiRestClient(Builder builder) {
    List<String> baseUrls = new LinkedList<>();
    for (String hostUrl : builder.hostUrls) {
      // Remove the trailing "/" from the hostUrl if present
      String baseUrl =
          String.format("%s/%s", hostUrl.replaceAll("/$", ""), builder.version.getApiNamespace());
      baseUrls.add(baseUrl);
    }

    RestConf conf = new RestConf();
    conf.setConnectTimeout(builder.connectTimeout);
    conf.setSocketTimeout(builder.socketTimeout);
    conf.setMaxAttempts(builder.maxAttempts);
    conf.setAttemptWaitTime(builder.attemptWaitTime);

    this.httpClient = RetryableRestClient.getRestClient(baseUrls, conf);

    switch (builder.authHeaderMethod) {
      case BASIC:
        this.authHeaderGenerator = new BasicAuthHeaderGenerator(builder.username, builder.password);
        break;

      case SPNEGO:
        this.authHeaderGenerator = new SpnegoAuthHeaderGenerator(builder.spnegoHost);
        break;

      default:
        if (builder.authHeaderGenerator == null) {
          this.authHeaderGenerator = new NoAuthHeaderGenerator();
        } else {
          this.authHeaderGenerator = builder.authHeaderGenerator;
        }
    }
  }

  public String getAuthHeader() {
    return authHeaderGenerator.generateAuthHeader();
  }

  public IRestClient getHttpClient() {
    return httpClient;
  }

  public static Builder builder(String hostUrl) {
    return new Builder(hostUrl);
  }

  public static Builder builder(String... hostUrls) {
    return new Builder(Arrays.asList(hostUrls));
  }

  public static Builder builder(List<String> hostUrls) {
    return new Builder(hostUrls);
  }

  public static class Builder {

    private List<String> hostUrls;

    private String spnegoHost;

    private ApiVersion version = ApiVersion.V1;

    private AuthHeaderMethod authHeaderMethod = AuthHeaderMethod.BASIC;

    private AuthHeaderGenerator authHeaderGenerator;

    private String username;

    private String password;

    private int socketTimeout = 3000;

    private int connectTimeout = 3000;

    private int maxAttempts = 3;

    private int attemptWaitTime = 3000;

    public Builder(String hostUrl) {
      if (hostUrl == null) {
        throw new IllegalArgumentException("hostUrl cannot be blank.");
      }
      this.hostUrls = new LinkedList<>();
      this.hostUrls.add(hostUrl);
    }

    public Builder(List<String> hostUrls) {
      if (hostUrls.isEmpty()) {
        throw new IllegalArgumentException("hostUrls cannot be blank.");
      }
      this.hostUrls = hostUrls;
    }

    public Builder spnegoHost(String host) {
      this.spnegoHost = host;
      return this;
    }

    public Builder apiVersion(ApiVersion version) {
      this.version = version;
      return this;
    }

    public Builder authHeaderMethod(AuthHeaderMethod authHeaderMethod) {
      this.authHeaderMethod = authHeaderMethod;
      return this;
    }

    /** Customize the AuthHeaderGenerator. */
    public Builder authHeaderGenerator(AuthHeaderGenerator authHeaderGenerator) {
      this.authHeaderGenerator = authHeaderGenerator;
      this.authHeaderMethod = AuthHeaderMethod.CUSTOM;
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

    public Builder maxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public Builder attemptWaitTime(int attemptWaitTime) {
      this.attemptWaitTime = attemptWaitTime;
      return this;
    }

    public KyuubiRestClient build() {
      if (authHeaderMethod == AuthHeaderMethod.SPNEGO && StringUtils.isBlank(spnegoHost)) {
        try {
          this.spnegoHost = new URI(hostUrls.get(0)).getHost();
        } catch (Exception e) {
          throw new IllegalArgumentException("spnegoHost is invalid.");
        }
      }
      return new KyuubiRestClient(this);
    }
  }
}
