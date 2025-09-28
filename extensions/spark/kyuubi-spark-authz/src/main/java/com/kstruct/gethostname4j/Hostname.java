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

package com.kstruct.gethostname4j;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;

// Alternative for RANGER-4125 to cut out JNA dependencies
public class Hostname {

  // The highest priority environment variable which allows user to
  // set hostname for Ranger client
  public static final String RANGER_CLIENT_HOSTNAME = "RANGER_CLIENT_HOSTNAME";

  /**
   * @return the hostname the of the current machine
   */
  public static String getHostname() {
    String hostname = System.getenv(RANGER_CLIENT_HOSTNAME);
    if (isValid(hostname)) return hostname;

    // Gets the host name from an environment variable
    // (COMPUTERNAME on Windows, HOSTNAME elsewhere)
    hostname = SystemUtils.getHostName();
    if (isValid(hostname)) return hostname;

    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException rethrow) {
      throw new RuntimeException(rethrow);
    }
  }

  private static boolean isValid(String hostname) {
    return StringUtils.isNotBlank(hostname) && !"localhost".equals(hostname);
  }
}
