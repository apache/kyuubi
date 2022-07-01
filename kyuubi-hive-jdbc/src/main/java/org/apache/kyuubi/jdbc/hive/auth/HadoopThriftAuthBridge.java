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

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Locale;
import java.util.Map;
import javax.security.auth.callback.*;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.SaslException;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.thrift.client.TUGIAssumingTransport;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functions that bridge Thrift's SASL transports to Hadoop's SASL callback handlers and
 * authentication classes. HIVE-11378 This class is not directly used anymore. It now exists only as
 * a shell to be extended by HadoopThriftAuthBridge23 in 0.23 shims. I have made it abstract to
 * avoid maintenance errors.
 */
public abstract class HadoopThriftAuthBridge {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopThriftAuthBridge.class);

  // We want to have only one auth bridge.  In the past this was handled by ShimLoader, but since
  // we're no longer using that we'll do it here.
  private static HadoopThriftAuthBridge self = null;

  public static HadoopThriftAuthBridge getBridge() {
    if (self == null) {
      synchronized (HadoopThriftAuthBridge.class) {
        if (self == null) self = new HadoopThriftAuthBridge23();
      }
    }
    return self;
  }

  public Client createClientWithConf(String authMethod) {
    UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      throw new IllegalStateException("Unable to get current login user: " + e, e);
    }
    if (loginUserHasCurrentAuthMethod(ugi, authMethod)) {
      LOG.debug("Not setting UGI conf as passed-in authMethod of " + authMethod + " = current.");
      return new Client();
    } else {
      LOG.debug("Setting UGI conf as passed-in authMethod of " + authMethod + " != current.");
      Configuration conf = new Configuration();
      conf.set(HADOOP_SECURITY_AUTHENTICATION, authMethod);
      UserGroupInformation.setConfiguration(conf);
      return new Client();
    }
  }

  public String getServerPrincipal(String principalConfig, String host) throws IOException {
    String serverPrincipal = SecurityUtil.getServerPrincipal(principalConfig, host);
    String names[] = SaslRpcServer.splitKerberosName(serverPrincipal);
    if (names.length != 3) {
      throw new IOException(
          "Kerberos principal name does NOT have the expected hostname part: " + serverPrincipal);
    }
    return serverPrincipal;
  }

  public UserGroupInformation getCurrentUGIWithConf(String authMethod) throws IOException {
    UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new IllegalStateException("Unable to get current user: " + e, e);
    }
    if (loginUserHasCurrentAuthMethod(ugi, authMethod)) {
      LOG.debug("Not setting UGI conf as passed-in authMethod of " + authMethod + " = current.");
      return ugi;
    } else {
      LOG.debug("Setting UGI conf as passed-in authMethod of " + authMethod + " != current.");
      Configuration conf = new Configuration();
      conf.set(HADOOP_SECURITY_AUTHENTICATION, authMethod);
      UserGroupInformation.setConfiguration(conf);
      return UserGroupInformation.getCurrentUser();
    }
  }

  /**
   * Return true if the current login user is already using the given authMethod.
   *
   * <p>Used above to ensure we do not create a new Configuration object and as such lose other
   * settings such as the cluster to which the JVM is connected. Required for oozie since it does
   * not have a core-site.xml see HIVE-7682
   */
  private boolean loginUserHasCurrentAuthMethod(UserGroupInformation ugi, String sAuthMethod) {
    AuthenticationMethod authMethod;
    try {
      // based on SecurityUtil.getAuthenticationMethod()
      authMethod =
          Enum.valueOf(AuthenticationMethod.class, sAuthMethod.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException iae) {
      throw new IllegalArgumentException(
          "Invalid attribute value for " + HADOOP_SECURITY_AUTHENTICATION + " of " + sAuthMethod,
          iae);
    }
    LOG.debug("Current authMethod = " + ugi.getAuthenticationMethod());
    return ugi.getAuthenticationMethod().equals(authMethod);
  }

  /**
   * Read and return Hadoop SASL configuration which can be configured using "hadoop.rpc.protection"
   *
   * @param conf
   * @return Hadoop SASL configuration
   */
  public abstract Map<String, String> getHadoopSaslProperties(Configuration conf);

  public static class Client {
    /**
     * Create a client-side SASL transport that wraps an underlying transport.
     *
     * @param methodStr The authentication method to use. Currently only KERBEROS is supported.
     * @param principalConfig The Kerberos principal of the target server.
     * @param underlyingTransport The underlying transport mechanism, usually a TSocket.
     * @param saslProps the sasl properties to create the client with
     */
    public TTransport createClientTransport(
        String principalConfig,
        String host,
        String methodStr,
        String tokenStrForm,
        final TTransport underlyingTransport,
        final Map<String, String> saslProps)
        throws IOException {
      final AuthMethod method = AuthMethod.valueOf(AuthMethod.class, methodStr);

      TTransport saslTransport = null;
      switch (method) {
        case DIGEST:
          Token<DelegationTokenIdentifier> t = new Token<>();
          t.decodeFromUrlString(tokenStrForm);
          saslTransport =
              new TSaslClientTransport(
                  method.getMechanismName(),
                  null,
                  null,
                  SaslRpcServer.SASL_DEFAULT_REALM,
                  saslProps,
                  new SaslClientCallbackHandler(t),
                  underlyingTransport);
          return new TUGIAssumingTransport(saslTransport, UserGroupInformation.getCurrentUser());

        case KERBEROS:
          String serverPrincipal = SecurityUtil.getServerPrincipal(principalConfig, host);
          final String names[] = SaslRpcServer.splitKerberosName(serverPrincipal);
          if (names.length != 3) {
            throw new IOException(
                "Kerberos principal name does NOT have the expected hostname part: "
                    + serverPrincipal);
          }
          try {
            return UserGroupInformation.getCurrentUser()
                .doAs(
                    new PrivilegedExceptionAction<TUGIAssumingTransport>() {
                      @Override
                      public TUGIAssumingTransport run() throws IOException {
                        TTransport saslTransport =
                            new TSaslClientTransport(
                                method.getMechanismName(),
                                null,
                                names[0],
                                names[1],
                                saslProps,
                                null,
                                underlyingTransport);
                        return new TUGIAssumingTransport(
                            saslTransport, UserGroupInformation.getCurrentUser());
                      }
                    });
          } catch (InterruptedException | SaslException se) {
            throw new IOException("Could not instantiate SASL transport", se);
          }

        default:
          throw new IOException("Unsupported authentication method: " + method);
      }
    }

    private static class SaslClientCallbackHandler implements CallbackHandler {
      private final String userName;
      private final char[] userPassword;

      public SaslClientCallbackHandler(Token<? extends TokenIdentifier> token) {
        this.userName = encodeIdentifier(token.getIdentifier());
        this.userPassword = encodePassword(token.getPassword());
      }

      @Override
      public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        NameCallback nc = null;
        PasswordCallback pc = null;
        RealmCallback rc = null;
        for (Callback callback : callbacks) {
          if (callback instanceof RealmChoiceCallback) {
            continue;
          } else if (callback instanceof NameCallback) {
            nc = (NameCallback) callback;
          } else if (callback instanceof PasswordCallback) {
            pc = (PasswordCallback) callback;
          } else if (callback instanceof RealmCallback) {
            rc = (RealmCallback) callback;
          } else {
            throw new UnsupportedCallbackException(callback, "Unrecognized SASL client callback");
          }
        }
        if (nc != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL client callback: setting username: " + userName);
          }
          nc.setName(userName);
        }
        if (pc != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL client callback: setting userPassword");
          }
          pc.setPassword(userPassword);
        }
        if (rc != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL client callback: setting realm: " + rc.getDefaultText());
          }
          rc.setText(rc.getDefaultText());
        }
      }

      static String encodeIdentifier(byte[] identifier) {
        return new String(Base64.encodeBase64(identifier));
      }

      static char[] encodePassword(byte[] password) {
        return new String(Base64.encodeBase64(password)).toCharArray();
      }
    }
  }
}
