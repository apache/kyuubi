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

import java.io.File;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.kyuubi.client.api.v1.dto.MultiPart;
import org.apache.kyuubi.client.exception.KyuubiRestException;
import org.apache.kyuubi.client.exception.RetryableKyuubiRestException;
import org.apache.kyuubi.client.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestClient implements IRestClient {

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

  @Override
  public <T> T get(
      String path,
      Map<String, Object> params,
      Class<T> type,
      String authHeader,
      Map<String, String> headers) {
    String responseJson = get(path, params, authHeader, headers);
    return JsonUtils.fromJson(responseJson, type);
  }

  @Override
  public String get(
      String path, Map<String, Object> params, String authHeader, Map<String, String> headers) {
    return doRequest(buildURI(path, params), authHeader, RequestBuilder.get(), headers);
  }

  @Override
  public <T> T post(
      String path, String body, Class<T> type, String authHeader, Map<String, String> headers) {
    String responseJson = post(path, body, authHeader, headers);
    return JsonUtils.fromJson(responseJson, type);
  }

  @Override
  public String post(String path, String body, String authHeader, Map<String, String> headers) {
    RequestBuilder postRequestBuilder = RequestBuilder.post();
    if (body != null) {
      postRequestBuilder.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
    }
    return doRequest(buildURI(path), authHeader, postRequestBuilder, headers);
  }

  @Override
  public <T> T post(
      String path,
      Map<String, MultiPart> multiPartMap,
      Class<T> type,
      String authHeader,
      Map<String, String> headers) {
    MultipartEntityBuilder entityBuilder =
        MultipartEntityBuilder.create().setCharset(StandardCharsets.UTF_8);
    multiPartMap.forEach(
        (s, multiPart) -> {
          ContentBody contentBody;
          Object payload = multiPart.getPayload();
          switch (multiPart.getType()) {
            case JSON:
              String string =
                  (payload instanceof String) ? (String) payload : JsonUtils.toJson(payload);
              contentBody = new StringBody(string, ContentType.APPLICATION_JSON);
              break;
            case FILE:
              contentBody = new FileBody((File) payload);
              break;
            default:
              throw new RuntimeException("Unsupported multi part type:" + multiPart.getType());
          }
          entityBuilder.addPart(s, contentBody);
        });
    HttpEntity httpEntity = entityBuilder.build();
    RequestBuilder postRequestBuilder = RequestBuilder.post(buildURI(path));
    postRequestBuilder.setHeader(httpEntity.getContentType());
    postRequestBuilder.setEntity(httpEntity);
    String responseJson = doRequest(buildURI(path), authHeader, postRequestBuilder, headers);
    return JsonUtils.fromJson(responseJson, type);
  }

  @Override
  public <T> T put(
      String path, String body, Class<T> type, String authHeader, Map<String, String> headers) {
    String responseJson = put(path, body, authHeader, headers);
    return JsonUtils.fromJson(responseJson, type);
  }

  @Override
  public String put(String path, String body, String authHeader, Map<String, String> headers) {
    RequestBuilder putRequestBuilder = RequestBuilder.put();
    if (body != null) {
      putRequestBuilder.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
    }
    return doRequest(buildURI(path), authHeader, putRequestBuilder, headers);
  }

  @Override
  public <T> T delete(
      String path,
      Map<String, Object> params,
      Class<T> type,
      String authHeader,
      Map<String, String> headers) {
    String responseJson = delete(path, params, authHeader, headers);
    return JsonUtils.fromJson(responseJson, type);
  }

  @Override
  public String delete(
      String path, Map<String, Object> params, String authHeader, Map<String, String> headers) {
    return doRequest(buildURI(path, params), authHeader, RequestBuilder.delete(), headers);
  }

  private String doRequest(
      URI uri, String authHeader, RequestBuilder requestBuilder, Map<String, String> headers) {
    String response;
    try {
      if (requestBuilder.getFirstHeader(HttpHeaders.CONTENT_TYPE) == null) {
        requestBuilder.setHeader(
            HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
      }
      headers.forEach(requestBuilder::setHeader);
      if (StringUtils.isNotBlank(authHeader)) {
        requestBuilder.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
      }
      HttpUriRequest httpRequest = requestBuilder.setUri(uri).build();

      LOG.debug("Executing {} request: {}", httpRequest.getMethod(), uri);

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
      LOG.debug("Response: {}", response);
    } catch (ConnectException
        | UnknownHostException
        | ConnectTimeoutException
        | NoHttpResponseException e) {
      // net exception can be retried by connecting to other Kyuubi server
      throw new RetryableKyuubiRestException("Api request failed for " + uri.toString(), e);
    } catch (KyuubiRestException rethrow) {
      throw rethrow;
    } catch (Exception e) {
      LOG.error("Error: ", e);
      throw new KyuubiRestException("Api request failed for " + uri.toString(), e);
    }

    return response;
  }

  private URI buildURI(String path) {
    return buildURI(path, null);
  }

  private URI buildURI(String path, Map<String, Object> params) {
    URI uri;
    try {
      String url = StringUtils.isNotBlank(path) ? this.baseUrl + "/" + path : this.baseUrl;
      URIBuilder builder = new URIBuilder(url);

      if (params != null) {
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
