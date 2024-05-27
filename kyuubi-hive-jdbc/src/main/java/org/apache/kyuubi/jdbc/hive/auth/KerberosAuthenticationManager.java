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

package org.apache.kyuubi.jdbc.hive.auth;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KerberosAuthenticationManager {

  private static final Map<String, CachingKerberosAuthentication> TGT_CACHE_AUTHENTICATION_CACHE =
      new ConcurrentHashMap<>();

  private static final Map<String, CachingKerberosAuthentication> KEYTAB_AUTHENTICATION_CACHE =
      new ConcurrentHashMap<>();

  public static CachingKerberosAuthentication getTgtCacheAuthentication(String ticketCache) {
    requireNonNull(ticketCache, "ticketCache is null");
    return TGT_CACHE_AUTHENTICATION_CACHE.computeIfAbsent(
        ticketCache,
        key -> {
          KerberosAuthentication tgtCacheAuth = new KerberosAuthentication(ticketCache);
          return new CachingKerberosAuthentication(tgtCacheAuth);
        });
  }

  public static CachingKerberosAuthentication getKeytabAuthentication(
      String principal, String keytab) {
    return KEYTAB_AUTHENTICATION_CACHE.computeIfAbsent(
        principal + ":" + keytab,
        key -> {
          KerberosAuthentication keytabAuth = new KerberosAuthentication(principal, keytab);
          return new CachingKerberosAuthentication(keytabAuth);
        });
  }
}
