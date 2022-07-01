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

package org.apache.kyuubi.jdbc.hive.server;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Preconditions;
import java.io.EOFException;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.JsonSerialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support for marshalling objects to and from JSON.
 *
 * <p>This extends {@link JsonSerialization} with the notion of a marker field in the JSON file,
 * with
 *
 * <ol>
 *   <li>a fail-fast check for it before even trying to parse.
 *   <li>Specific IOException subclasses for a failure.
 * </ol>
 *
 * <p>The rationale for this is not only to support different things in the, registry, but the fact
 * that all ZK nodes have a size &gt; 0 when examined.
 *
 * @param <T> Type to marshal.
 */
public class JsonSerDeser<T> {
  private static final Logger LOG = LoggerFactory.getLogger(JsonSerDeser.class);
  private static final String UTF_8 = "UTF-8";
  public static final String E_NO_DATA = "No data at path";
  public static final String E_DATA_TOO_SHORT = "Data at path too short";
  public static final String E_MISSING_MARKER_STRING = "Missing marker string: ";

  private final Class<T> classType;
  private final ObjectMapper mapper;

  /**
   * Create an instance bound to a specific type
   *
   * @param classType class to marshall
   */
  public JsonSerDeser(Class<T> classType) {
    Preconditions.checkArgument(classType != null, "null classType");
    this.classType = classType;
    this.mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationFeature.INDENT_OUTPUT, false);
  }

  /**
   * Deserialize from a byte array
   *
   * @param path path the data came from
   * @param bytes byte array
   * @throws IOException all problems
   * @throws EOFException not enough data
   * @throws InvalidRecordException if the parsing failed -the record is invalid
   * @throws NoRecordException if the data is not considered a record: either it is too short or it
   *     did not contain the marker string.
   */
  public T fromBytes(String path, byte[] bytes) throws IOException {
    return fromBytes(path, bytes, "");
  }

  /**
   * Deserialize from a byte array, optionally checking for a marker string.
   *
   * <p>If the marker parameter is supplied (and not empty), then its presence will be verified
   * before the JSON parsing takes place; it is a fast-fail check. If not found, an {@link
   * InvalidRecordException} exception will be raised
   *
   * @param path path the data came from
   * @param bytes byte array
   * @param marker an optional string which, if set, MUST be present in the UTF-8 parsed payload.
   * @return The parsed record
   * @throws IOException all problems
   * @throws EOFException not enough data
   * @throws InvalidRecordException if the JSON parsing failed.
   * @throws NoRecordException if the data is not considered a record: either it is too short or it
   *     did not contain the marker string.
   */
  public T fromBytes(String path, byte[] bytes, String marker) throws IOException {
    int len = bytes.length;
    if (len == 0) {
      throw new NoRecordException(path, E_NO_DATA);
    }
    if (StringUtils.isNotEmpty(marker) && len < marker.length()) {
      throw new NoRecordException(path, E_DATA_TOO_SHORT);
    }
    String json = new String(bytes, 0, len, UTF_8);
    if (StringUtils.isNotEmpty(marker) && !json.contains(marker)) {
      throw new NoRecordException(path, E_MISSING_MARKER_STRING + marker);
    }
    try {
      return fromJson(json);
    } catch (JsonProcessingException e) {
      throw new InvalidRecordException(path, e.toString(), e);
    }
  }

  /**
   * Convert an instance to a string form for output. This is a robust operation which will convert
   * any JSON-generating exceptions into error text.
   *
   * @param instance non-null instance
   * @return a JSON string
   */
  public String toString(T instance) {
    Preconditions.checkArgument(instance != null, "Null instance argument");
    try {
      return toJson(instance);
    } catch (JsonProcessingException e) {
      return "Failed to convert to a string: " + e;
    }
  }

  /**
   * Convert an instance to a JSON string.
   *
   * @param instance instance to convert
   * @return a JSON string description
   * @throws JsonProcessingException Json generation problems
   */
  public synchronized String toJson(T instance) throws JsonProcessingException {
    return mapper.writeValueAsString(instance);
  }

  /**
   * Convert from JSON.
   *
   * @param json input
   * @return the parsed JSON
   * @throws IOException IO problems
   * @throws JsonParseException If the input is not well-formatted
   * @throws JsonMappingException failure to map from the JSON to this class
   */
  public synchronized T fromJson(String json)
      throws IOException, JsonParseException, JsonMappingException {
    if (json.isEmpty()) {
      throw new EOFException("No data");
    }
    try {
      return mapper.readValue(json, classType);
    } catch (IOException e) {
      LOG.error("Exception while parsing json : {}\n{}", e, json, e);
      throw e;
    }
  }

  /**
   * Convert JSON to bytes.
   *
   * @param instance instance to convert
   * @return a byte array
   * @throws IOException IO problems
   */
  public byte[] toBytes(T instance) throws IOException {
    return mapper.writeValueAsBytes(instance);
  }
}
