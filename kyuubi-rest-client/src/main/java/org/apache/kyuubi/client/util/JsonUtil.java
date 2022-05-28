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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kyuubi.client.exception.KyuubiRestException;

public final class JsonUtil {

  private static ObjectMapper MAPPER = new ObjectMapper();

  public static String toJson(Object object) throws KyuubiRestException {
    try {
      return MAPPER.writeValueAsString(object);
    } catch (Exception e) {
      throw new KyuubiRestException("Failed to convert object to json", e);
    }
  }

  public static <T> T toObject(String json, Class<T> clazz) throws KyuubiRestException {
    try {
      return MAPPER.readValue(json, clazz);
    } catch (Exception e) {
      throw new KyuubiRestException("Failed to convert json string to object", e);
    }
  }
}
