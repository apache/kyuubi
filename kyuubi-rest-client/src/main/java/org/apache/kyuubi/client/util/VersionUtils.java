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

import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersionUtils {
  static final Logger LOG = LoggerFactory.getLogger(VersionUtils.class);

  public static final String KYUUBI_CLIENT_VERSION_KEY = "kyuubi.client.version";
  private static String KYUUBI_CLIENT_VERSION;

  public static synchronized String getVersion() {
    if (KYUUBI_CLIENT_VERSION == null) {
      try {
        Properties prop = new Properties();
        prop.load(VersionUtils.class.getClassLoader().getResourceAsStream("version.properties"));
        KYUUBI_CLIENT_VERSION = prop.getProperty(KYUUBI_CLIENT_VERSION_KEY, "unknown");
      } catch (Exception e) {
        LOG.error("Error getting kyuubi client version", e);
        KYUUBI_CLIENT_VERSION = "unknown";
      }
    }
    return KYUUBI_CLIENT_VERSION;
  }
}
