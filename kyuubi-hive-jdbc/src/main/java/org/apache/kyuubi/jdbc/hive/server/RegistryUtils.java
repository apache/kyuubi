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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.IDN;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods for working with a registry. */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RegistryUtils {
  private static final Logger LOG = LoggerFactory.getLogger(RegistryUtils.class);

  /**
   * Convert the username to that which can be used for registry entries. Lower cases it, Strip the
   * kerberos realm off a username if needed, and any "/" hostname entries
   *
   * @param username user
   * @return the converted username
   */
  public static String convertUsername(String username) {
    String converted = org.apache.hadoop.util.StringUtils.toLowerCase(username);
    int atSymbol = converted.indexOf('@');
    if (atSymbol > 0) {
      converted = converted.substring(0, atSymbol);
    }
    int slashSymbol = converted.indexOf('/');
    if (slashSymbol > 0) {
      converted = converted.substring(0, slashSymbol);
    }
    return converted;
  }

  /**
   * Get the current username, before any encoding has been applied.
   *
   * @return the current user from the kerberos identity, falling back to the user and/or env
   *     variables.
   */
  private static String currentUsernameUnencoded() {
    String env_hadoop_username = System.getenv(RegistryInternalConstants.HADOOP_USER_NAME);
    return getCurrentUsernameUnencoded(env_hadoop_username);
  }

  /**
   * Get the current username, using the value of the parameter <code>env_hadoop_username</code> if
   * it is set on an insecure cluster. This ensures that the username propagates correctly across
   * processes started by YARN.
   *
   * <p>This method is primarly made visible for testing.
   *
   * @param env_hadoop_username the environment variable
   * @return the selected username
   * @throws RuntimeException if there is a problem getting the short user name of the current user.
   */
  @VisibleForTesting
  public static String getCurrentUsernameUnencoded(String env_hadoop_username) {
    String shortUserName = null;
    if (!UserGroupInformation.isSecurityEnabled()) {
      shortUserName = env_hadoop_username;
    }
    if (StringUtils.isEmpty(shortUserName)) {
      try {
        shortUserName = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return shortUserName;
  }

  /**
   * Get the current user path formatted for the registry
   *
   * <p>In an insecure cluster, the environment variable <code>HADOOP_USER_NAME </code> is queried
   * <i>first</i>.
   *
   * <p>This means that in a YARN container where the creator set this environment variable to
   * propagate their identity, the defined user name is used in preference to the actual user.
   *
   * <p>In a secure cluster, the kerberos identity of the current user is used.
   *
   * @return the encoded shortname of the current user
   * @throws RuntimeException if the current user identity cannot be determined from the
   *     OS/kerberos.
   */
  public static String currentUser() {
    String shortUserName = currentUsernameUnencoded();
    return registryUser(shortUserName);
  }

  /**
   * Convert the given user name formatted for the registry.
   *
   * @param shortUserName
   * @return converted user name
   */
  public static String registryUser(String shortUserName) {
    String encodedName = IDN.toASCII(shortUserName);
    // DNS name doesn't allow "_", replace it with "-"
    encodedName = RegistryUtils.convertUsername(encodedName);
    return encodedName.replace("_", "-");
  }

  /** Static instance of service record marshalling */
  public static class ServiceRecordMarshal extends JsonSerDeser<ServiceRecord> {
    public ServiceRecordMarshal() {
      super(ServiceRecord.class);
    }
  }
}
