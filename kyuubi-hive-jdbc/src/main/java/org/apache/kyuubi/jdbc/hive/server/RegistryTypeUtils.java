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

import static org.apache.kyuubi.jdbc.hive.server.AddressTypes.*;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Static methods to work with registry types —primarily endpoints and the list representation of
 * addresses.
 */
public class RegistryTypeUtils {

  /**
   * Create an IPC endpoint
   *
   * @param api API
   * @param address the address as a tuple of (hostname, port)
   * @return the new endpoint
   */
  public static Endpoint ipcEndpoint(String api, InetSocketAddress address) {
    return new Endpoint(
        api,
        ADDRESS_HOSTNAME_AND_PORT,
        "hadoop/IPC",
        address == null ? null : hostnamePortPair(address));
  }

  /**
   * Create a single entry map
   *
   * @param key map entry key
   * @param val map entry value
   * @return a 1 entry map.
   */
  public static Map<String, String> map(String key, String val) {
    Map<String, String> map = new HashMap<String, String>(1);
    map.put(key, val);
    return map;
  }

  /**
   * Create a URI
   *
   * @param uri value
   * @return a 1 entry map.
   */
  public static Map<String, String> uri(String uri) {
    return map(ADDRESS_URI, uri);
  }

  /**
   * Create a (hostname, port) address pair
   *
   * @param hostname hostname
   * @param port port
   * @return a 1 entry map.
   */
  public static Map<String, String> hostnamePortPair(String hostname, int port) {
    Map<String, String> map = map(ADDRESS_HOSTNAME_FIELD, hostname);
    map.put(ADDRESS_PORT_FIELD, Integer.toString(port));
    return map;
  }

  /**
   * Create a (hostname, port) address pair
   *
   * @param address socket address whose hostname and port are used for the generated address.
   * @return a 1 entry map.
   */
  public static Map<String, String> hostnamePortPair(InetSocketAddress address) {
    return hostnamePortPair(address.getHostName(), address.getPort());
  }

  /**
   * Get a specific field from an address -raising an exception if the field is not present
   *
   * @param address address to query
   * @param field field to resolve
   * @return the resolved value. Guaranteed to be non-null.
   * @throws InvalidRecordException if the field did not resolve
   */
  public static String getAddressField(Map<String, String> address, String field)
      throws InvalidRecordException {
    String val = address.get(field);
    if (val == null) {
      throw new InvalidRecordException("", "Missing address field: " + field);
    }
    return val;
  }

  /**
   * Validate the record by checking for null fields and other invalid conditions
   *
   * @param path path for exceptions
   * @param record record to validate. May be null
   * @throws InvalidRecordException on invalid entries
   */
  public static void validateServiceRecord(String path, ServiceRecord record)
      throws InvalidRecordException {
    if (record == null) {
      throw new InvalidRecordException(path, "Null record");
    }
    if (!ServiceRecord.RECORD_TYPE.equals(record.type)) {
      throw new InvalidRecordException(path, "invalid record type field: \"" + record.type + "\"");
    }

    if (record.external != null) {
      for (Endpoint endpoint : record.external) {
        validateEndpoint(path, endpoint);
      }
    }
    if (record.internal != null) {
      for (Endpoint endpoint : record.internal) {
        validateEndpoint(path, endpoint);
      }
    }
  }

  /**
   * Validate the endpoint by checking for null fields and other invalid conditions
   *
   * @param path path for exceptions
   * @param endpoint endpoint to validate. May be null
   * @throws InvalidRecordException on invalid entries
   */
  public static void validateEndpoint(String path, Endpoint endpoint)
      throws InvalidRecordException {
    if (endpoint == null) {
      throw new InvalidRecordException(path, "Null endpoint");
    }
    try {
      endpoint.validate();
    } catch (RuntimeException e) {
      throw new InvalidRecordException(path, e.toString());
    }
  }
}
