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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.kyuubi.client.exception.KyuubiRestException;
import org.apache.kyuubi.client.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestClient implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(RestClient.class);

  private CloseableHttpClient httpclient;

  private String baseUrl;

  public RestClient(String baseUrl, CloseableHttpClient httpclient) {
    this.httpclient = httpclient;
    this.baseUrl = baseUrl;
  }

  @Override
  public void close() throws Exception {
    if (httpclient != null) {
      httpclient.close();
    }
  }

  public <T> T get(String path, Map<String, Object> params, Class<T> type, String authHeader) {
    String responseJson = get(path, params, authHeader);
    return JsonUtil.toObject(responseJson, type);
  }

  public String get(String path, Map<String, Object> params, String authHeader) {
    return doRequest(buildURI(path, params), authHeader, RequestBuilder.get());
  }

  public <T> T post(String path, String body, Class<T> type, String authHeader) {
    String responseJson = post(path, body, authHeader);
    return JsonUtil.toObject(responseJson, type);
  }

  public String post(String path, String body, String authHeader) {
    RequestBuilder postRequestBuilder =
        RequestBuilder.post().setEntity(new StringEntity(body, StandardCharsets.UTF_8));
    return doRequest(buildURI(path), authHeader, postRequestBuilder);
  }

  public <T> T delete(String path, Map<String, Object> params, Class<T> type, String authHeader) {
    String responseJson = delete(path, params, authHeader);
    return JsonUtil.toObject(responseJson, type);
  }

  public String delete(String path, Map<String, Object> params, String authHeader) {
    return doRequest(buildURI(path, params), authHeader, RequestBuilder.delete());
  }

  private String doRequest(URI uri, String authHeader, RequestBuilder requestBuilder) {
    String response = "";
    CloseableHttpResponse httpResponse = null;
    try {
      if (StringUtils.isNotBlank(authHeader)) {
        requestBuilder.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
      }
      HttpUriRequest httpRequest =
          requestBuilder
              .setUri(uri)
              .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
              .build();

      LOG.info("Executing {} request: {}", httpRequest.getMethod(), uri);

      ResponseHandler<String> responseHandler =
          resp -> {
            int status = resp.getStatusLine().getStatusCode();
            HttpEntity entity = resp.getEntity();
            String entityStr = entity != null ? EntityUtils.toString(entity) : null;
            if (status >= 200 && status < 300) {
              return entityStr;
            } else {
              throw new HttpResponseException(status, entityStr);
            }
          };

      response = httpclient.execute(httpRequest, responseHandler);
      LOG.info("Response: {}", response);
    } catch (Exception e) {
      LOG.error("Error: ", e);
      throw new KyuubiRestException("Api request failed for " + uri.toString(), e);
    } finally {
      if (httpResponse != null) {
        try {
          httpResponse.close();
        } catch (IOException e) {
          throw new KyuubiRestException("Failed to close HttpResponse.", e);
        }
      }
    }

    return response;
  }

  private URI buildURI(String path) {
    return buildURI(path, null);
  }

  private URI buildURI(String path, Map<String, Object> params) {
    URI uri = null;
    try {
      String url = StringUtils.isNotBlank(path) ? this.baseUrl + "/" + path : this.baseUrl;
      URIBuilder builder = new URIBuilder(url);

      if (MapUtils.isNotEmpty(params)) {
        for (Map.Entry<String, Object> entry : params.entrySet()) {
          if (entry.getValue() != null) {
            builder.addParameter(entry.getKey(), entry.getValue().toString());
          }
        }
      }

      uri = builder.build();
    } catch (URISyntaxException e) {
      throw new KyuubiRestException("invalid URI.", e);
    }

    return uri;
  }
}
