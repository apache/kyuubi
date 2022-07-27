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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KerberosAuthenticationManager {

  private static CachingKerberosAuthentication GLOBAL_TGT_CACHE_AUTHENTICATION;

  private static final Map<String, CachingKerberosAuthentication> KEYTAB_AUTHENTICATION_CACHE =
      new ConcurrentHashMap<>();

  public static synchronized CachingKerberosAuthentication getTgtCacheAuthentication() {
    if (GLOBAL_TGT_CACHE_AUTHENTICATION == null) {
      KerberosAuthentication tgtCacheAuth = new KerberosAuthentication();
      GLOBAL_TGT_CACHE_AUTHENTICATION = new CachingKerberosAuthentication(tgtCacheAuth);
    }
    return GLOBAL_TGT_CACHE_AUTHENTICATION;
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
