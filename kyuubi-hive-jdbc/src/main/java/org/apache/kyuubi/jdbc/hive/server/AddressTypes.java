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

/**
 * Enum of address types -as integers. Why integers and not enums? Cross platform serialization as
 * JSON
 */
public interface AddressTypes {

  /**
   * hostname/FQDN and port pair: {@value}. The host/domain name and port are set as separate
   * strings in the address list, e.g.
   *
   * <pre>
   *   ["namenode.example.org", "9870"]
   * </pre>
   */
  String ADDRESS_HOSTNAME_AND_PORT = "host/port";

  String ADDRESS_HOSTNAME_FIELD = "host";
  String ADDRESS_PORT_FIELD = "port";

  /**
   * URI entries: {@value}.
   *
   * <pre>
   *   ["http://example.org"]
   * </pre>
   */
  String ADDRESS_URI = "uri";
}
