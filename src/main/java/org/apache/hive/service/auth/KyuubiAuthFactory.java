/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.service.auth;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.HadoopShims.KerberosNameShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge.Server.ServerMode;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.spark.KyuubiConf;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkUtils;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

/**
 * This class helps in some aspects of authentication. It creates the proper Thrift classes for the
 * given configuration as well as helps with authenticating requests.
 */
public class KyuubiAuthFactory {
  public enum AuthTypes {
    NONE("NONE"),
    KERBEROS("KERBEROS");

    private final String authType;

    AuthTypes(String authType) {
      this.authType = authType;
    }

    public String getAuthName() {
      return authType;
    }
  }

  private HadoopThriftAuthBridge.Server saslServer;
  private String authTypeStr;
  private final SparkConf conf;

  public static final String HS2_PROXY_USER = "hive.server2.proxy.user";
  public static final String KYUUBI_CLIENT_TOKEN = "kyuubiClientToken";

  public KyuubiAuthFactory(SparkConf conf) throws TTransportException {
    this.conf = conf;
    authTypeStr = conf.get(KyuubiConf.AUTHENTICATION_METHOD().key());
    String keytab = conf.get(KyuubiConf.KEYTAB().key(), "");
    String principal = conf.get(KyuubiConf.PRINCIPAL().key(), "");

    if (authTypeStr == null || keytab.isEmpty() || principal.isEmpty()) {
      authTypeStr = AuthTypes.NONE.getAuthName();
    }

    if (authTypeStr.equalsIgnoreCase(AuthTypes.KERBEROS.getAuthName())) {
      saslServer = ShimLoader.getHadoopThriftAuthBridge().createServer(keytab, principal);
      // start delegation token manager
      try {
        saslServer.startDelegationTokenSecretManager(
          SparkUtils.newConfiguration(conf),
          null,
          ServerMode.HIVESERVER2);
      } catch (IOException e) {
        throw new TTransportException("Failed to start token manager", e);
      }
    }
  }

  private Map<String, String> getSaslProperties() {
    Map<String, String> saslProps = new HashMap<>();
    SaslQOP saslQOP = SaslQOP.fromString(conf.get(KyuubiConf.SASL_QOP()));
    saslProps.put(Sasl.QOP, saslQOP.toString());
    saslProps.put(Sasl.SERVER_AUTH, "true");
    return saslProps;
  }

  public TTransportFactory getAuthTransFactory() throws LoginException {
    TTransportFactory transportFactory;
    if (authTypeStr.equalsIgnoreCase(AuthTypes.KERBEROS.getAuthName())) {
      try {
        transportFactory = saslServer.createTransportFactory(getSaslProperties());
      } catch (TTransportException e) {
        throw new LoginException(e.getMessage());
      }
    } else if (authTypeStr.equalsIgnoreCase(AuthTypes.NONE.getAuthName())) {
      transportFactory = KyuubiPlainSaslHelper.getPlainTransportFactory(authTypeStr);
    } else {
      throw new LoginException("Unsupported authentication type " + authTypeStr);
    }
    return transportFactory;
  }

  /**
   * Returns the thrift processor factory for HiveServer2 running in binary mode
   * @param service
   * @return
   */
  public TProcessorFactory getAuthProcFactory(TCLIService.Iface service) {
    if (authTypeStr.equalsIgnoreCase(AuthTypes.KERBEROS.getAuthName())) {
      return KyuubiKerberosSaslHelper.getKerberosProcessorFactory(saslServer, service);
    } else {
      return KyuubiPlainSaslHelper.getPlainProcessorFactory(service);
    }
  }

  public String getRemoteUser() {
    return saslServer == null ? null : saslServer.getRemoteUser();
  }

  public String getIpAddress() {
    if (saslServer == null || saslServer.getRemoteAddress() == null) {
      return null;
    } else {
      return saslServer.getRemoteAddress().getHostAddress();
    }
  }

  public static TServerSocket getServerSocket(String hiveHost, int portNum)
    throws TTransportException {
    InetSocketAddress serverAddress;
    if (hiveHost == null || hiveHost.isEmpty()) {
      // Wildcard bind
      serverAddress = new InetSocketAddress(portNum);
    } else {
      serverAddress = new InetSocketAddress(hiveHost, portNum);
    }
    return new TServerSocket(serverAddress);
  }

  public static void verifyProxyAccess(String realUser, String proxyUser, String ipAddress,
    HiveConf hiveConf) throws HiveSQLException {
    try {
      UserGroupInformation sessionUgi;
      if (UserGroupInformation.isSecurityEnabled()) {
        KerberosNameShim kerbName = ShimLoader.getHadoopShims().getKerberosNameShim(realUser);
        sessionUgi = UserGroupInformation.createProxyUser(
            kerbName.getServiceName(), UserGroupInformation.getLoginUser());
      } else {
        sessionUgi = UserGroupInformation.createRemoteUser(realUser);
      }
      if (!proxyUser.equalsIgnoreCase(realUser)) {
        ProxyUsers.refreshSuperUserGroupsConfiguration(hiveConf);
        ProxyUsers.authorize(UserGroupInformation.createProxyUser(proxyUser, sessionUgi),
            ipAddress, hiveConf);
      }
    } catch (IOException e) {
      throw new HiveSQLException(
        "Failed to validate proxy privilege of " + realUser + " for " + proxyUser, "08S01", e);
    }
  }

  // retrieve delegation token for the given user
  public String getDelegationToken(String owner, String renewer) throws HiveSQLException {
    if (saslServer == null) {
      throw new HiveSQLException(
        "Delegation token only supported over kerberos authentication", "08S01");
    }

    try {
      String tokenStr = saslServer.getDelegationTokenWithService(owner, renewer, KYUUBI_CLIENT_TOKEN);
      if (tokenStr == null || tokenStr.isEmpty()) {
        throw new HiveSQLException(
          "Received empty retrieving delegation token for user " + owner, "08S01");
      }
      return tokenStr;
    } catch (IOException e) {
      throw new HiveSQLException(
        "Error retrieving delegation token for user " + owner, "08S01", e);
    } catch (InterruptedException e) {
      throw new HiveSQLException("delegation token retrieval interrupted", "08S01", e);
    }
  }

  // cancel given delegation token
  public void cancelDelegationToken(String delegationToken) throws HiveSQLException {
    if (saslServer == null) {
      throw new HiveSQLException(
        "Delegation token only supported over kerberos authentication", "08S01");
    }
    try {
      saslServer.cancelDelegationToken(delegationToken);
    } catch (IOException e) {
      throw new HiveSQLException(
        "Error canceling delegation token " + delegationToken, "08S01", e);
    }
  }

  public void renewDelegationToken(String delegationToken) throws HiveSQLException {
    if (saslServer == null) {
      throw new HiveSQLException(
        "Delegation token only supported over kerberos authentication", "08S01");
    }
    try {
      saslServer.renewDelegationToken(delegationToken);
    } catch (IOException e) {
      throw new HiveSQLException(
        "Error renewing delegation token " + delegationToken, "08S01", e);
    }
  }
}
