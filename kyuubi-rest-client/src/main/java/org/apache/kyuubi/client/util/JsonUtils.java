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

package org.apache.kyuubi.client.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kyuubi.client.exception.KyuubiRestException;

public final class JsonUtils {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  public static String toJson(Object object) {
    try {
      return MAPPER.writeValueAsString(object);
    } catch (Exception e) {
      throw new KyuubiRestException(
          String.format("Failed to convert object(%s) to json", object), e);
    }
  }

  public static String toPrettyJson(Object object) {
    try {
      return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(object);
    } catch (Exception e) {
      throw new KyuubiRestException(
          String.format("Failed to convert object(%s) to json", object), e);
    }
  }

  public static <T> T fromJson(String json, Class<T> clazz) {
    try {
      return MAPPER.readValue(json, clazz);
    } catch (Exception e) {
      throw new KyuubiRestException(
          String.format("Failed to convert json string(%s) to %s", json, clazz.getName()), e);
    }
  }
}
