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
import org.apache.kyuubi.client.api.v1.dto.MultiPart;

/** A underlying http client interface for common rest request. */
public interface IRestClient extends AutoCloseable {
  <T> T get(
      String path,
      Map<String, Object> params,
      Class<T> type,
      String authHeader,
      Map<String, String> headers);

  default <T> T get(String path, Map<String, Object> params, Class<T> type, String authHeader) {
    return get(path, params, type, authHeader, Collections.emptyMap());
  }

  String get(
      String path, Map<String, Object> params, String authHeader, Map<String, String> headers);

  default String get(String path, Map<String, Object> params, String authHeader) {
    return get(path, params, authHeader, Collections.emptyMap());
  }

  <T> T post(
      String path, String body, Class<T> type, String authHeader, Map<String, String> headers);

  default <T> T post(String path, String body, Class<T> type, String authHeader) {
    return post(path, body, type, authHeader, Collections.emptyMap());
  }

  <T> T post(
      String path,
      Map<String, MultiPart> multiPartMap,
      Class<T> type,
      String authHeader,
      Map<String, String> headers);

  default <T> T post(
      String path, Map<String, MultiPart> multiPartMap, Class<T> type, String authHeader) {
    return post(path, multiPartMap, type, authHeader, Collections.emptyMap());
  }

  String post(String path, String body, String authHeader, Map<String, String> headers);

  default String post(String path, String body, String authHeader) {
    return post(path, body, authHeader, Collections.emptyMap());
  }

  <T> T put(
      String path, String body, Class<T> type, String authHeader, Map<String, String> headers);

  default <T> T put(String path, String body, Class<T> type, String authHeader) {
    return put(path, body, type, authHeader, Collections.emptyMap());
  }

  String put(String path, String body, String authHeader, Map<String, String> headers);

  default String put(String path, String body, String authHeader) {
    return put(path, body, authHeader, Collections.emptyMap());
  }

  <T> T delete(
      String path,
      Map<String, Object> params,
      Class<T> type,
      String authHeader,
      Map<String, String> headers);

  default <T> T delete(String path, Map<String, Object> params, Class<T> type, String authHeader) {
    return delete(path, params, type, authHeader, Collections.emptyMap());
  }

  String delete(
      String path, Map<String, Object> params, String authHeader, Map<String, String> headers);

  default String delete(String path, Map<String, Object> params, String authHeader) {
    return delete(path, params, authHeader, Collections.emptyMap());
  }
}
