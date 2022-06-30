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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperUtils.class);

  public ZookeeperUtils() {}

  public static String setupZookeeperAuth(
      Configuration conf, String saslLoginContextName, String zkPrincipal, String zkKeytab)
      throws IOException {
    if (UserGroupInformation.isSecurityEnabled() && saslLoginContextName != null) {
      LOG.info("UGI security is enabled. Setting up ZK auth.");
      if (zkPrincipal != null && !zkPrincipal.isEmpty()) {
        if (zkKeytab != null && !zkKeytab.isEmpty()) {
          return setZookeeperClientKerberosJaasConfig(saslLoginContextName, zkPrincipal, zkKeytab);
        } else {
          throw new IOException("Kerberos keytab is empty");
        }
      } else {
        throw new IOException("Kerberos principal is empty");
      }
    } else {
      LOG.info(
          "UGI security is not enabled, or no SASL context name. Skipping setting up ZK auth.");
      return null;
    }
  }

  private static String setZookeeperClientKerberosJaasConfig(
      String saslLoginContextName, String zkPrincipal, String zkKeytab) throws IOException {
    System.setProperty("zookeeper.sasl.clientconfig", saslLoginContextName);
    String principal = SecurityUtil.getServerPrincipal(zkPrincipal, "0.0.0.0");
    ZookeeperUtils.JaasConfiguration jaasConf =
        new ZookeeperUtils.JaasConfiguration(saslLoginContextName, principal, zkKeytab);
    javax.security.auth.login.Configuration.setConfiguration(jaasConf);
    return principal;
  }

  private static class JaasConfiguration extends javax.security.auth.login.Configuration {
    private final javax.security.auth.login.Configuration baseConfig =
        javax.security.auth.login.Configuration.getConfiguration();
    private final String loginContextName;
    private final String principal;
    private final String keyTabFile;

    public JaasConfiguration(String loginContextName, String principal, String keyTabFile) {
      this.loginContextName = loginContextName;
      this.principal = principal;
      this.keyTabFile = keyTabFile;
    }

    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      if (this.loginContextName.equals(appName)) {
        Map<String, String> krbOptions = new HashMap();
        krbOptions.put("doNotPrompt", "true");
        krbOptions.put("storeKey", "true");
        krbOptions.put("useKeyTab", "true");
        krbOptions.put("principal", this.principal);
        krbOptions.put("keyTab", this.keyTabFile);
        krbOptions.put("refreshKrb5Config", "true");
        AppConfigurationEntry zooKeeperClientEntry =
            new AppConfigurationEntry(
                KerberosUtil.getKrb5LoginModuleName(), LoginModuleControlFlag.REQUIRED, krbOptions);
        return new AppConfigurationEntry[] {zooKeeperClientEntry};
      } else {
        return this.baseConfig != null ? this.baseConfig.getAppConfigurationEntry(appName) : null;
      }
    }
  }
}
