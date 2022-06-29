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

package org.apache.kyuubi.jdbc.service.auth;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Locale;
import java.util.Map;
import javax.security.auth.callback.*;
import javax.security.sasl.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.thrift.client.TUGIAssumingTransport;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SaslRpcServer.SaslGssCallbackHandler;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.TSaslServerTransport.Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HadoopThriftAuthBridge {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopThriftAuthBridge.class);
  private static HadoopThriftAuthBridge self = null;

  public HadoopThriftAuthBridge() {}

  public static HadoopThriftAuthBridge getBridge() {
    if (self == null) {
      Class var0 = HadoopThriftAuthBridge.class;
      synchronized (HadoopThriftAuthBridge.class) {
        if (self == null) {
          self = new HadoopThriftAuthBridge23();
        }
      }
    }

    return self;
  }

  public HadoopThriftAuthBridge.Client createClient() {
    return new HadoopThriftAuthBridge.Client();
  }

  public HadoopThriftAuthBridge.Client createClientWithConf(String authMethod) {
    UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.getLoginUser();
    } catch (IOException var4) {
      throw new IllegalStateException("Unable to get current login user: " + var4, var4);
    }

    if (this.loginUserHasCurrentAuthMethod(ugi, authMethod)) {
      LOG.debug("Not setting UGI conf as passed-in authMethod of " + authMethod + " = current.");
      return new HadoopThriftAuthBridge.Client();
    } else {
      LOG.debug("Setting UGI conf as passed-in authMethod of " + authMethod + " != current.");
      Configuration conf = new Configuration();
      conf.set("hadoop.security.authentication", authMethod);
      UserGroupInformation.setConfiguration(conf);
      return new HadoopThriftAuthBridge.Client();
    }
  }

  public HadoopThriftAuthBridge.Server createServer(
      String keytabFile, String principalConf, String clientConf) throws TTransportException {
    return new HadoopThriftAuthBridge.Server(keytabFile, principalConf, clientConf);
  }

  public String getServerPrincipal(String principalConfig, String host) throws IOException {
    String serverPrincipal = SecurityUtil.getServerPrincipal(principalConfig, host);
    String[] names = SaslRpcServer.splitKerberosName(serverPrincipal);
    if (names.length != 3) {
      throw new IOException(
          "Kerberos principal name does NOT have the expected hostname part: " + serverPrincipal);
    } else {
      return serverPrincipal;
    }
  }

  public String getCanonicalHostName(String hostName) {
    try {
      return InetAddress.getByName(hostName).getCanonicalHostName();
    } catch (UnknownHostException var3) {
      LOG.warn("Could not retrieve canonical hostname for " + hostName, var3);
      return hostName;
    }
  }

  public UserGroupInformation getCurrentUGIWithConf(String authMethod) throws IOException {
    UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException var4) {
      throw new IllegalStateException("Unable to get current user: " + var4, var4);
    }

    if (this.loginUserHasCurrentAuthMethod(ugi, authMethod)) {
      LOG.debug("Not setting UGI conf as passed-in authMethod of " + authMethod + " = current.");
      return ugi;
    } else {
      LOG.debug("Setting UGI conf as passed-in authMethod of " + authMethod + " != current.");
      Configuration conf = new Configuration();
      conf.set("hadoop.security.authentication", authMethod);
      UserGroupInformation.setConfiguration(conf);
      return UserGroupInformation.getCurrentUser();
    }
  }

  private boolean loginUserHasCurrentAuthMethod(UserGroupInformation ugi, String sAuthMethod) {
    AuthenticationMethod authMethod;
    try {
      authMethod =
          (AuthenticationMethod)
              Enum.valueOf(AuthenticationMethod.class, sAuthMethod.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException var5) {
      throw new IllegalArgumentException(
          "Invalid attribute value for hadoop.security.authentication of " + sAuthMethod, var5);
    }

    LOG.debug("Current authMethod = " + ugi.getAuthenticationMethod());
    return ugi.getAuthenticationMethod().equals(authMethod);
  }

  public abstract Map<String, String> getHadoopSaslProperties(Configuration var1);

  public static class Server {
    protected final UserGroupInformation realUgi;
    protected final UserGroupInformation clientValidationUGI;
    protected DelegationTokenSecretManager secretManager;
    static final ThreadLocal<InetAddress> remoteAddress =
        new ThreadLocal<InetAddress>() {
          protected InetAddress initialValue() {
            return null;
          }
        };
    static final ThreadLocal<AuthenticationMethod> authenticationMethod =
        new ThreadLocal<AuthenticationMethod>() {
          protected AuthenticationMethod initialValue() {
            return AuthenticationMethod.TOKEN;
          }
        };
    private static ThreadLocal<String> remoteUser =
        new ThreadLocal<String>() {
          protected String initialValue() {
            return null;
          }
        };
    private static final ThreadLocal<String> userAuthMechanism =
        new ThreadLocal<String>() {
          protected String initialValue() {
            return AuthMethod.KERBEROS.getMechanismName();
          }
        };

    public Server() throws TTransportException {
      try {
        this.realUgi = UserGroupInformation.getCurrentUser();
        this.clientValidationUGI = UserGroupInformation.getCurrentUser();
      } catch (IOException var2) {
        throw new TTransportException(var2);
      }
    }

    protected Server(String keytabFile, String principalConf, String clientConf)
        throws TTransportException {
      if (keytabFile != null && !keytabFile.isEmpty()) {
        if (principalConf != null && !principalConf.isEmpty()) {
          if (clientConf == null || clientConf.isEmpty()) {
            HadoopThriftAuthBridge.LOG.warn(
                "Client-facing principal not set. Using server-side setting: " + principalConf);
            clientConf = principalConf;
          }

          try {
            HadoopThriftAuthBridge.LOG.info("Logging in via CLIENT based principal ");
            String kerberosName = SecurityUtil.getServerPrincipal(clientConf, "0.0.0.0");
            UserGroupInformation.loginUserFromKeytab(kerberosName, keytabFile);
            this.clientValidationUGI = UserGroupInformation.getLoginUser();

            assert this.clientValidationUGI.isFromKeytab();

            HadoopThriftAuthBridge.LOG.info("Logging in via SERVER based principal ");
            kerberosName = SecurityUtil.getServerPrincipal(principalConf, "0.0.0.0");
            UserGroupInformation.loginUserFromKeytab(kerberosName, keytabFile);
            this.realUgi = UserGroupInformation.getLoginUser();

            assert this.realUgi.isFromKeytab();

          } catch (IOException var6) {
            throw new TTransportException(var6);
          }
        } else {
          throw new TTransportException("No principal specified");
        }
      } else {
        throw new TTransportException("No keytab specified");
      }
    }

    public void setSecretManager(DelegationTokenSecretManager secretManager) {
      this.secretManager = secretManager;
    }

    public TTransportFactory createTransportFactory(Map<String, String> saslProps)
        throws TTransportException {
      Factory transFactory = this.createSaslServerTransportFactory(saslProps);
      return new HadoopThriftAuthBridge.Server.TUGIAssumingTransportFactory(
          transFactory, this.clientValidationUGI);
    }

    public Factory createSaslServerTransportFactory(Map<String, String> saslProps)
        throws TTransportException {
      String kerberosName = this.clientValidationUGI.getUserName();
      String[] names = SaslRpcServer.splitKerberosName(kerberosName);
      if (names.length != 3) {
        throw new TTransportException("Kerberos principal should have 3 parts: " + kerberosName);
      } else {
        Factory transFactory = new Factory();
        transFactory.addServerDefinition(
            AuthMethod.KERBEROS.getMechanismName(),
            names[0],
            names[1],
            saslProps,
            new SaslGssCallbackHandler());
        transFactory.addServerDefinition(
            AuthMethod.DIGEST.getMechanismName(),
            (String) null,
            "default",
            saslProps,
            new HadoopThriftAuthBridge.Server.SaslDigestCallbackHandler(this.secretManager));
        return transFactory;
      }
    }

    public TTransportFactory wrapTransportFactory(TTransportFactory transFactory) {
      return new HadoopThriftAuthBridge.Server.TUGIAssumingTransportFactory(
          transFactory, this.realUgi);
    }

    public TProcessor wrapProcessor(TProcessor processor) {
      return new HadoopThriftAuthBridge.Server.TUGIAssumingProcessor(
          processor, this.secretManager, true);
    }

    public TProcessor wrapNonAssumingProcessor(TProcessor processor) {
      return new HadoopThriftAuthBridge.Server.TUGIAssumingProcessor(
          processor, this.secretManager, false);
    }

    public InetAddress getRemoteAddress() {
      return (InetAddress) remoteAddress.get();
    }

    public String getRemoteUser() {
      return (String) remoteUser.get();
    }

    public String getUserAuthMechanism() {
      return (String) userAuthMechanism.get();
    }

    static class TUGIAssumingTransportFactory extends TTransportFactory {
      private final UserGroupInformation ugi;
      private final TTransportFactory wrapped;

      public TUGIAssumingTransportFactory(TTransportFactory wrapped, UserGroupInformation ugi) {
        assert wrapped != null;

        assert ugi != null;

        this.wrapped = wrapped;
        this.ugi = ugi;
      }

      public TTransport getTransport(final TTransport trans) {
        return (TTransport)
            this.ugi.doAs(
                new PrivilegedAction<TTransport>() {
                  public TTransport run() {
                    return TUGIAssumingTransportFactory.this.wrapped.getTransport(trans);
                  }
                });
      }
    }

    protected class TUGIAssumingProcessor implements TProcessor {
      final TProcessor wrapped;
      DelegationTokenSecretManager secretManager;
      boolean useProxy;

      TUGIAssumingProcessor(
          TProcessor wrapped, DelegationTokenSecretManager secretManager, boolean useProxy) {
        this.wrapped = wrapped;
        this.secretManager = secretManager;
        this.useProxy = useProxy;
      }

      public boolean process(final TProtocol inProt, final TProtocol outProt) throws TException {
        TTransport trans = inProt.getTransport();
        if (!(trans instanceof TSaslServerTransport)) {
          throw new TException("Unexpected non-SASL transport " + trans.getClass());
        } else {
          TSaslServerTransport saslTrans = (TSaslServerTransport) trans;
          SaslServer saslServer = saslTrans.getSaslServer();
          String authId = saslServer.getAuthorizationID();
          HadoopThriftAuthBridge.LOG.debug("AUTH ID ======>" + authId);
          String endUser = authId;
          Socket socket = ((TSocket) ((TSocket) saslTrans.getUnderlyingTransport())).getSocket();
          HadoopThriftAuthBridge.Server.remoteAddress.set(socket.getInetAddress());
          String mechanismName = saslServer.getMechanismName();
          HadoopThriftAuthBridge.Server.userAuthMechanism.set(mechanismName);
          if (AuthMethod.PLAIN.getMechanismName().equalsIgnoreCase(mechanismName)) {
            HadoopThriftAuthBridge.Server.remoteUser.set(authId);
            return this.wrapped.process(inProt, outProt);
          } else {
            HadoopThriftAuthBridge.Server.authenticationMethod.set(AuthenticationMethod.KERBEROS);
            if (AuthMethod.TOKEN.getMechanismName().equalsIgnoreCase(mechanismName)) {
              try {
                TokenIdentifier tokenId = SaslRpcServer.getIdentifier(authId, this.secretManager);
                endUser = tokenId.getUser().getUserName();
                HadoopThriftAuthBridge.Server.authenticationMethod.set(AuthenticationMethod.TOKEN);
              } catch (InvalidToken var25) {
                throw new TException(var25.getMessage());
              }
            }

            UserGroupInformation clientUgi = null;

            boolean var12;
            try {
              if (this.useProxy) {
                clientUgi =
                    UserGroupInformation.createProxyUser(
                        endUser, UserGroupInformation.getLoginUser());
                HadoopThriftAuthBridge.Server.remoteUser.set(clientUgi.getShortUserName());
                HadoopThriftAuthBridge.LOG.debug(
                    "Set remoteUser :" + (String) HadoopThriftAuthBridge.Server.remoteUser.get());
                boolean var31 =
                    (Boolean)
                        clientUgi.doAs(
                            new PrivilegedExceptionAction<Boolean>() {
                              public Boolean run() {
                                try {
                                  return TUGIAssumingProcessor.this.wrapped.process(
                                      inProt, outProt);
                                } catch (TException var2) {
                                  throw new RuntimeException(var2);
                                }
                              }
                            });
                return var31;
              }

              UserGroupInformation endUserUgi = UserGroupInformation.createRemoteUser(endUser);
              HadoopThriftAuthBridge.Server.remoteUser.set(endUserUgi.getShortUserName());
              HadoopThriftAuthBridge.LOG.debug(
                  "Set remoteUser :"
                      + (String) HadoopThriftAuthBridge.Server.remoteUser.get()
                      + ", from endUser :"
                      + endUser);
              var12 = this.wrapped.process(inProt, outProt);
            } catch (RuntimeException var26) {
              if (var26.getCause() instanceof TException) {
                throw (TException) var26.getCause();
              }

              throw var26;
            } catch (InterruptedException var27) {
              throw new RuntimeException(var27);
            } catch (IOException var28) {
              throw new RuntimeException(var28);
            } finally {
              if (clientUgi != null) {
                try {
                  FileSystem.closeAllForUGI(clientUgi);
                } catch (IOException var24) {
                  HadoopThriftAuthBridge.LOG.error(
                      "Could not clean up file-system handles for UGI: " + clientUgi, var24);
                }
              }
            }

            return var12;
          }
        }
      }
    }

    static class SaslDigestCallbackHandler implements CallbackHandler {
      private final DelegationTokenSecretManager secretManager;

      public SaslDigestCallbackHandler(DelegationTokenSecretManager secretManager) {
        this.secretManager = secretManager;
      }

      private char[] getPassword(DelegationTokenIdentifier tokenid) throws InvalidToken {
        return this.encodePassword(this.secretManager.retrievePassword(tokenid));
      }

      private char[] encodePassword(byte[] password) {
        return (new String(Base64.encodeBase64(password))).toCharArray();
      }

      public void handle(Callback[] callbacks) throws InvalidToken, UnsupportedCallbackException {
        NameCallback nc = null;
        PasswordCallback pc = null;
        AuthorizeCallback ac = null;
        Callback[] var5 = callbacks;
        int var6 = callbacks.length;

        for (int var7 = 0; var7 < var6; ++var7) {
          Callback callback = var5[var7];
          if (callback instanceof AuthorizeCallback) {
            ac = (AuthorizeCallback) callback;
          } else if (callback instanceof NameCallback) {
            nc = (NameCallback) callback;
          } else if (callback instanceof PasswordCallback) {
            pc = (PasswordCallback) callback;
          } else if (!(callback instanceof RealmCallback)) {
            throw new UnsupportedCallbackException(
                callback, "Unrecognized SASL DIGEST-MD5 Callback");
          }
        }

        if (pc != null) {
          DelegationTokenIdentifier tokenIdentifier =
              (DelegationTokenIdentifier)
                  SaslRpcServer.getIdentifier(nc.getDefaultName(), this.secretManager);
          char[] password = this.getPassword(tokenIdentifier);
          if (HadoopThriftAuthBridge.LOG.isDebugEnabled()) {
            HadoopThriftAuthBridge.LOG.debug(
                "SASL server DIGEST-MD5 callback: setting password for client: "
                    + tokenIdentifier.getUser());
          }

          pc.setPassword(password);
        }

        if (ac != null) {
          String authid = ac.getAuthenticationID();
          String authzid = ac.getAuthorizationID();
          if (authid.equals(authzid)) {
            ac.setAuthorized(true);
          } else {
            ac.setAuthorized(false);
          }

          if (ac.isAuthorized()) {
            if (HadoopThriftAuthBridge.LOG.isDebugEnabled()) {
              String username =
                  ((DelegationTokenIdentifier)
                          SaslRpcServer.getIdentifier(authzid, this.secretManager))
                      .getUser()
                      .getUserName();
              HadoopThriftAuthBridge.LOG.debug(
                  "SASL server DIGEST-MD5 callback: setting canonicalized client ID: " + username);
            }

            ac.setAuthorizedID(authzid);
          }
        }
      }
    }

    public static enum ServerMode {
      HIVESERVER2,
      METASTORE;

      private ServerMode() {}
    }
  }

  public static class Client {
    public Client() {}

    public TTransport createClientTransport(
        String principalConfig,
        String host,
        String methodStr,
        String tokenStrForm,
        final TTransport underlyingTransport,
        final Map<String, String> saslProps)
        throws IOException {
      final AuthMethod method = (AuthMethod) AuthMethod.valueOf(AuthMethod.class, methodStr);
      TTransport saslTransport = null;
      switch (method) {
        case DIGEST:
          Token<DelegationTokenIdentifier> t = new Token();
          t.decodeFromUrlString(tokenStrForm);
          saslTransport =
              new TSaslClientTransport(
                  method.getMechanismName(),
                  (String) null,
                  (String) null,
                  "default",
                  saslProps,
                  new HadoopThriftAuthBridge.Client.SaslClientCallbackHandler(t),
                  underlyingTransport);
          return new TUGIAssumingTransport(saslTransport, UserGroupInformation.getCurrentUser());
        case KERBEROS:
          String serverPrincipal = SecurityUtil.getServerPrincipal(principalConfig, host);
          final String[] names = SaslRpcServer.splitKerberosName(serverPrincipal);
          if (names.length != 3) {
            throw new IOException(
                "Kerberos principal name does NOT have the expected hostname part: "
                    + serverPrincipal);
          } else {
            try {
              return (TTransport)
                  UserGroupInformation.getCurrentUser()
                      .doAs(
                          new PrivilegedExceptionAction<TUGIAssumingTransport>() {
                            public TUGIAssumingTransport run() throws IOException {
                              TTransport saslTransport =
                                  new TSaslClientTransport(
                                      method.getMechanismName(),
                                      (String) null,
                                      names[0],
                                      names[1],
                                      saslProps,
                                      (CallbackHandler) null,
                                      underlyingTransport);
                              return new TUGIAssumingTransport(
                                  saslTransport, UserGroupInformation.getCurrentUser());
                            }
                          });
            } catch (SaslException | InterruptedException var13) {
              throw new IOException("Could not instantiate SASL transport", var13);
            }
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

      public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        NameCallback nc = null;
        PasswordCallback pc = null;
        RealmCallback rc = null;
        Callback[] var5 = callbacks;
        int var6 = callbacks.length;

        for (int var7 = 0; var7 < var6; ++var7) {
          Callback callback = var5[var7];
          if (!(callback instanceof RealmChoiceCallback)) {
            if (callback instanceof NameCallback) {
              nc = (NameCallback) callback;
            } else if (callback instanceof PasswordCallback) {
              pc = (PasswordCallback) callback;
            } else {
              if (!(callback instanceof RealmCallback)) {
                throw new UnsupportedCallbackException(
                    callback, "Unrecognized SASL client callback");
              }

              rc = (RealmCallback) callback;
            }
          }
        }

        if (nc != null) {
          if (HadoopThriftAuthBridge.LOG.isDebugEnabled()) {
            HadoopThriftAuthBridge.LOG.debug(
                "SASL client callback: setting username: " + this.userName);
          }

          nc.setName(this.userName);
        }

        if (pc != null) {
          if (HadoopThriftAuthBridge.LOG.isDebugEnabled()) {
            HadoopThriftAuthBridge.LOG.debug("SASL client callback: setting userPassword");
          }

          pc.setPassword(this.userPassword);
        }

        if (rc != null) {
          if (HadoopThriftAuthBridge.LOG.isDebugEnabled()) {
            HadoopThriftAuthBridge.LOG.debug(
                "SASL client callback: setting realm: " + rc.getDefaultText());
          }

          rc.setText(rc.getDefaultText());
        }
      }

      static String encodeIdentifier(byte[] identifier) {
        return new String(Base64.encodeBase64(identifier));
      }

      static char[] encodePassword(byte[] password) {
        return (new String(Base64.encodeBase64(password))).toCharArray();
      }
    }
  }
}
