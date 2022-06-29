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

package org.apache.kyuubi.service.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

/** HiveServer2. */
public class HiveServer2 {
  public static final String INSTANCE_URI_CONFIG = "hive.server2.instance.uri";

  public static boolean isHTTPTransportMode(Configuration hiveConf) {
    String transportMode = System.getenv("HIVE_SERVER2_TRANSPORT_MODE");
    if (transportMode == null) {
      transportMode = hiveConf.get(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname);
    }
    if (transportMode != null && (transportMode.equalsIgnoreCase("http"))) {
      return true;
    }
    return false;
  }

  public static boolean isKerberosAuthMode(Configuration hiveConf) {
    String authMode = hiveConf.get(ConfVars.HIVE_SERVER2_AUTHENTICATION.varname);
    if (authMode != null && (authMode.equalsIgnoreCase("KERBEROS"))) {
      return true;
    }
    return false;
  }

  interface FailoverHandler {
    void failover() throws Exception;
  }
}
