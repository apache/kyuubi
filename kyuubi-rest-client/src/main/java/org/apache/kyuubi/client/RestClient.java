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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.kyuubi.client.exception.KyuubiRestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestClient implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(RestClient.class);

  private CloseableHttpClient httpclient;

  private String baseUrl;

  private String authHeader;

  public RestClient(String baseUrl, CloseableHttpClient httpclient, String authHeader) {
    this.httpclient = httpclient;
    this.baseUrl = baseUrl;
    this.authHeader = authHeader;
  }

  @Override
  public void close() throws Exception {
    if (httpclient != null) {
      httpclient.close();
    }
  }

  public <T> T get(String path, Map<String, Object> params, TypeReference<T> type)
      throws KyuubiRestException {
    try {
      String responseJson = get(path, params);
      return new ObjectMapper().readValue(responseJson, type);
    } catch (JsonProcessingException e) {
      throw new KyuubiRestException(
          "cannot convert response json to object: " + type.getType().getTypeName(), e);
    }
  }

  public String get(String path, Map<String, Object> params) throws KyuubiRestException {
    return doRequest(buildURI(path, params), RequestBuilder.get());
  }

  public <T> T post(String body, TypeReference<T> type) throws KyuubiRestException {
    try {
      String responseJson = post(body);
      return new ObjectMapper().readValue(responseJson, type);
    } catch (JsonProcessingException e) {
      throw new KyuubiRestException(
          "cannot convert response json to object: " + type.getType().getTypeName(), e);
    }
  }

  public String post(String body) throws KyuubiRestException {
    RequestBuilder postRequestBuilder =
        RequestBuilder.post().setEntity(new StringEntity(body, StandardCharsets.UTF_8));
    return doRequest(buildURI(), postRequestBuilder);
  }

  public String delete(String path, Map<String, Object> params) throws KyuubiRestException {
    return doRequest(buildURI(path, params), RequestBuilder.delete());
  }

  private String doRequest(URI uri, RequestBuilder requestBuilder) throws KyuubiRestException {
    String response = "";
    CloseableHttpResponse httpResponse = null;
    try {
      HttpUriRequest httpRequest =
          requestBuilder
              .setUri(uri)
              .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
              .setHeader(HttpHeaders.AUTHORIZATION, authHeader)
              .build();

      LOG.info("Executing {} request: {}", httpRequest.getMethod(), uri);
      httpResponse = httpclient.execute(httpRequest);
      response = new BasicResponseHandler().handleResponse(httpResponse);
      LOG.info("Response: {}", response);
    } catch (Exception e) {
      e.printStackTrace();
      throw new KyuubiRestException("Api request failed for " + uri.toString(), e);
    } finally {
      if (httpResponse != null) {
        try {
          httpResponse.close();
        } catch (IOException e) {
          throw new KyuubiRestException("Close http response failed.", e);
        }
      }
    }

    return response;
  }

  private URI buildURI() throws KyuubiRestException {
    return buildURI(null, null);
  }

  private URI buildURI(String path, Map<String, Object> params) throws KyuubiRestException {
    URI uri = null;
    try {
      String url = StringUtils.isNotBlank(path) ? this.baseUrl + "/" + path : this.baseUrl;
      URIBuilder builder = new URIBuilder(url);

      if (MapUtils.isNotEmpty(params)) {
        for (Map.Entry<String, Object> entry : params.entrySet()) {
          builder.addParameter(entry.getKey(), entry.getValue().toString());
        }
      }

      uri = builder.build();
    } catch (URISyntaxException e) {
      throw new KyuubiRestException("invalid URI.", e);
    }

    return uri;
  }
}
