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

import java.util.List;
import java.util.Map;

/**
 * Description of a single service/component endpoint. It is designed to be marshalled as JSON.
 *
 * <p>Every endpoint can have more than one address entry, such as a list of URLs to a replicated
 * service, or a (hostname, port) pair. Each of these address entries is represented as a string
 * list, as that is the only reliably marshallable form of a tuple JSON can represent.
 */
public final class Endpoint implements Cloneable {

  /** API implemented at the end of the binding */
  public String api;

  /** Type of address. The standard types are defined in {@link AddressTypes} */
  public String addressType;

  /** Protocol type. Some standard types are defined in {@link ProtocolTypes} */
  public String protocolType;

  /** a list of address tuples —tuples whose format depends on the address type */
  public List<Map<String, String>> addresses;

  /** Create an empty instance. */
  public Endpoint() {}

  @Override
  public String toString() {
    return marshalToString.toString(this);
  }

  /**
   * Shallow clone: the lists of addresses are shared
   *
   * @return a cloned instance
   * @throws CloneNotSupportedException
   */
  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  /** Static instance of service record marshalling */
  private static class Marshal extends JsonSerDeser<Endpoint> {
    private Marshal() {
      super(Endpoint.class);
    }
  }

  private static final Marshal marshalToString = new Marshal();
}
