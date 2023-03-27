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

import java.util.Map;
import org.apache.kyuubi.client.api.v1.dto.MultiPart;

/** A underlying http client interface for common rest request. */
public interface IRestClient extends AutoCloseable {
  <T> T get(String path, Map<String, Object> params, Class<T> type, String authHeader);

  String get(String path, Map<String, Object> params, String authHeader);

  <T> T post(String path, String body, Class<T> type, String authHeader);

  <T> T post(String path, Map<String, MultiPart> multiPartMap, Class<T> type, String authHeader);

  String post(String path, String body, String authHeader);

  <T> T put(String path, String body, Class<T> type, String authHeader);

  String put(String path, String body, String authHeader);

  <T> T delete(String path, Map<String, Object> params, Class<T> type, String authHeader);

  String delete(String path, Map<String, Object> params, String authHeader);
}
