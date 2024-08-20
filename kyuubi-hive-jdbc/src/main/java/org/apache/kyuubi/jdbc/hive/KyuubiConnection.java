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

package org.apache.kyuubi.jdbc.hive;

import static org.apache.kyuubi.jdbc.hive.JdbcConnectionParams.*;
import static org.apache.kyuubi.jdbc.hive.Utils.HIVE_SERVER2_RETRY_KEY;
import static org.apache.kyuubi.jdbc.hive.Utils.HIVE_SERVER2_RETRY_TRUE;

import com.google.common.base.Preconditions;
import java.io.*;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantLock;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContexts;
import org.apache.kyuubi.jdbc.hive.adapter.SQLConnection;
import org.apache.kyuubi.jdbc.hive.auth.*;
import org.apache.kyuubi.jdbc.hive.cli.FetchType;
import org.apache.kyuubi.jdbc.hive.cli.RowSet;
import org.apache.kyuubi.jdbc.hive.cli.RowSetFactory;
import org.apache.kyuubi.jdbc.hive.logs.KyuubiLoggable;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.*;
import org.apache.kyuubi.shaded.thrift.TConfiguration;
import org.apache.kyuubi.shaded.thrift.TException;
import org.apache.kyuubi.shaded.thrift.protocol.TBinaryProtocol;
import org.apache.kyuubi.shaded.thrift.transport.THttpClient;
import org.apache.kyuubi.shaded.thrift.transport.TTransport;
import org.apache.kyuubi.shaded.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** KyuubiConnection. */
public class KyuubiConnection implements SQLConnection, KyuubiLoggable {
  public static final Logger LOG = LoggerFactory.getLogger(KyuubiConnection.class.getName());
  public static final String BEELINE_MODE_PROPERTY = "BEELINE_MODE";
  public static final String HS2_PROXY_USER = "hive.server2.proxy.user";
  public static int DEFAULT_ENGINE_LOG_THREAD_TIMEOUT = 10 * 1000;

  private String jdbcUriString;
  private String host;
  private int port;
  private final Map<String, String> sessConfMap;
  private JdbcConnectionParams connParams;
  private TTransport transport;
  private TCLIService.Iface client;
  private boolean isClosed = true;
  private SQLWarning warningChain = null;
  private TSessionHandle sessHandle = null;
  private final List<TProtocolVersion> supportedProtocols = new LinkedList<>();
  private int connectTimeout = 0;
  private int socketTimeout = 0;
  private TProtocolVersion protocol;
  private int fetchSize = KyuubiStatement.DEFAULT_FETCH_SIZE;
  private String initFile = null;
  private String wmPool = null, wmApp = null;
  private Properties clientInfo;
  private boolean initFileCompleted = false;
  private TOperationHandle launchEngineOpHandle = null;
  private Thread engineLogThread;
  private boolean engineLogInflight = true;
  private volatile boolean launchEngineOpCompleted = false;
  private boolean launchEngineOpSupportResult = false;
  private String engineId = "";
  private String engineName = "";
  private String engineUrl = "";
  private String engineRefId = "";

  private boolean isBeeLineMode;

  /** Get all direct HiveServer2 URLs from a ZooKeeper based HiveServer2 URL */
  public static List<JdbcConnectionParams> getAllUrls(String zookeeperBasedHS2Url)
      throws Exception {
    JdbcConnectionParams params = Utils.parseURL(zookeeperBasedHS2Url, new Properties());
    // if zk is disabled or if HA service discovery is enabled we return the already populated
    // params.
    // in HA mode, params is already populated with Active server host info.
    if (params.getZooKeeperEnsemble() == null) {
      return Collections.singletonList(params);
    }
    return ZooKeeperHiveClientHelper.getDirectParamsList(params);
  }

  public KyuubiConnection(String uri, Properties info) throws SQLException {
    isBeeLineMode = Boolean.parseBoolean(info.getProperty(BEELINE_MODE_PROPERTY));
    try {
      connParams = Utils.parseURL(uri, info);
    } catch (ZooKeeperHiveClientException e) {
      throw new KyuubiSQLException(e);
    }
    jdbcUriString = connParams.getJdbcUriString();
    sessConfMap = connParams.getSessionVars();

    if (!sessConfMap.containsKey(AUTH_PRINCIPAL)
        && sessConfMap.containsKey(AUTH_KYUUBI_SERVER_PRINCIPAL)) {
      sessConfMap.put(AUTH_PRINCIPAL, sessConfMap.get(AUTH_KYUUBI_SERVER_PRINCIPAL));
    }

    // JDBC URL: jdbc:hive2://<host>:<port>/dbName;sess_var_list?hive_conf_list#hive_var_list
    // each list: <key1>=<val1>;<key2>=<val2> and so on
    // sess_var_list -> sessConfMap
    // hive_conf_list -> hiveConfMap
    // hive_var_list -> hiveVarMap
    if (isKerberosAuthMode()) {
      host = Utils.getCanonicalHostName(connParams.getHost());
    } else {
      host = connParams.getHost();
    }
    port = connParams.getPort();

    setupTimeout();

    if (sessConfMap.containsKey(FETCH_SIZE)) {
      fetchSize = Integer.parseInt(sessConfMap.get(FETCH_SIZE));
    }
    if (sessConfMap.containsKey(INIT_FILE)) {
      initFile = sessConfMap.get(INIT_FILE);
    }
    wmPool = sessConfMap.get(WM_POOL);
    for (String application : APPLICATION) {
      wmApp = sessConfMap.get(application);
      if (wmApp != null) break;
    }

    // add supported protocols
    Collections.addAll(supportedProtocols, TProtocolVersion.values());

    int maxRetries = 1;
    try {
      String strRetries = sessConfMap.get(RETRIES);
      if (StringUtils.isNotBlank(strRetries)) {
        maxRetries = Integer.parseInt(strRetries);
      }
    } catch (NumberFormatException e) { // Ignore the exception
    }

    for (int numRetries = 0; ; ) {
      try {
        // open the client transport
        openTransport();
        // set up the client
        TCLIService.Iface _client = new TCLIService.Client(new TBinaryProtocol(transport));
        // Wrap the client with a thread-safe proxy to serialize the RPC calls
        client = newSynchronizedClient(_client);
        // open client session
        openSession();
        if (!isBeeLineMode) {
          showLaunchEngineLog();
          waitLaunchEngineToComplete();
          executeInitSql();
        }
        break;
      } catch (Exception e) {
        LOG.warn("Failed to connect to " + connParams.getHost() + ":" + connParams.getPort());
        String errMsg = null;
        String warnMsg = "Could not open client transport with JDBC Uri: " + jdbcUriString + ": ";
        try {
          close();
        } catch (Exception ex) {
          // Swallow the exception
          LOG.debug("Error while closing the connection", ex);
        }
        if (ZooKeeperHiveClientHelper.isZkDynamicDiscoveryMode(sessConfMap)) {
          errMsg = "Could not open client transport for any of the Server URI's in ZooKeeper: ";
          // Try next available server in zookeeper, or retry all the servers again if retry is
          // enabled
          while (!Utils.updateConnParamsFromZooKeeper(connParams) && ++numRetries < maxRetries) {
            connParams.getRejectedHostZnodePaths().clear();
          }
          // Update with new values
          jdbcUriString = connParams.getJdbcUriString();
          if (isKerberosAuthMode()) {
            host = Utils.getCanonicalHostName(connParams.getHost());
          } else {
            host = connParams.getHost();
          }
          port = connParams.getPort();
        } else {
          errMsg = warnMsg;
          ++numRetries;
        }

        if (numRetries >= maxRetries) {
          throw new KyuubiSQLException(errMsg + e.getMessage(), "08S01", e);
        } else {
          LOG.warn(warnMsg + e.getMessage() + " Retrying " + numRetries + " of " + maxRetries);
        }
      }
    }
  }

  /**
   * Check whether launch engine operation might be producing more logs to be fetched. This method
   * is a public API for usage outside of Kyuubi, although it is not part of the interface
   * java.sql.Connection.
   *
   * @return true if launch engine operation might be producing more logs. It does not indicate if
   *     last log lines have been fetched by getEngineLog.
   */
  public boolean hasMoreLogs() {
    return launchEngineOpHandle != null && (engineLogInflight || !launchEngineOpCompleted);
  }

  /**
   * Get the launch engine operation logs of current connection. This method is a public API for
   * usage outside of Kyuubi, although it is not part of the interface java.sql.Connection. This
   * method gets the incremental logs during launching engine, and uses fetchSize holden by
   * KyuubiStatement object.
   *
   * @return a list of logs. It can be empty if there are no new logs to be retrieved at that time.
   * @throws SQLException
   * @throws ClosedOrCancelledException if connection has been closed
   */
  @Override
  public List<String> getExecLog() throws SQLException, ClosedOrCancelledException {
    if (isClosed()) {
      throw new ClosedOrCancelledException(
          "Method getExecLog() failed. The " + "connection has been closed.");
    }

    if (launchEngineOpHandle == null) {
      return Collections.emptyList();
    }

    TFetchResultsReq fetchResultsReq =
        new TFetchResultsReq(launchEngineOpHandle, TFetchOrientation.FETCH_NEXT, fetchSize);
    fetchResultsReq.setFetchType(FetchType.LOG.toTFetchType());

    List<String> logs = new ArrayList<>();
    try {
      TFetchResultsResp tFetchResultsResp = client.FetchResults(fetchResultsReq);
      RowSet rowSet = RowSetFactory.create(tFetchResultsResp.getResults(), this.getProtocol());
      for (Object[] row : rowSet) {
        logs.add(String.valueOf(row[0]));
      }
    } catch (TException e) {
      throw new KyuubiSQLException("Error building result set for query log", e);
    }
    engineLogInflight = !logs.isEmpty();
    return Collections.unmodifiableList(logs);
  }

  private void showLaunchEngineLog() {
    if (launchEngineOpHandle != null) {
      LOG.info("Starting to get launch engine log.");
      engineLogThread =
          new Thread("engine-launch-log") {

            @Override
            public void run() {
              try {
                while (hasMoreLogs()) {
                  List<String> logs = getExecLog();
                  for (String log : logs) {
                    LOG.info(log);
                  }
                  Thread.sleep(300);
                }
              } catch (Exception e) {
                // do nothing
              }
              LOG.info("Finished to get launch engine log.");
            }
          };
      engineLogThread.start();
    }
  }

  public void setEngineLogThread(Thread logThread) {
    this.engineLogThread = logThread;
  }

  public void executeInitSql() throws SQLException {
    if (initFileCompleted) return;
    if (initFile != null) {
      try {
        List<String> sqlList = parseInitFile(initFile);
        try (Statement st = createStatement()) {
          for (String sql : sqlList) {
            boolean hasResult = st.execute(sql);
            if (hasResult) {
              try (ResultSet rs = st.getResultSet()) {
                while (rs.next()) {
                  System.out.println(rs.getString(1));
                }
              }
            }
          }
        }
      } catch (Exception e) {
        LOG.error("Failed to execute initial SQL");
        throw new KyuubiSQLException(e.getMessage());
      }
    }
    initFileCompleted = true;
  }

  public static List<String> parseInitFile(String initFile) throws IOException {
    File file = new File(initFile);
    BufferedReader br = null;
    List<String> initSqlList = null;
    try {
      FileInputStream input = new FileInputStream(file);
      br = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
      String line;
      StringBuilder sb = new StringBuilder();
      while ((line = br.readLine()) != null) {
        line = line.trim();
        if (line.length() != 0) {
          if (line.startsWith("#") || line.startsWith("--")) {
            continue;
          } else {
            line = line.concat(" ");
            sb.append(line);
          }
        }
      }
      initSqlList = getInitSql(sb.toString());
    } catch (IOException e) {
      LOG.error("Failed to read initial SQL file", e);
      throw new IOException(e);
    } finally {
      if (br != null) {
        br.close();
      }
    }
    return initSqlList;
  }

  private static List<String> getInitSql(String sbLine) {
    char[] sqlArray = sbLine.toCharArray();
    List<String> initSqlList = new ArrayList<>();
    int index = 0;
    int beginIndex = 0;
    for (; index < sqlArray.length; index++) {
      if (sqlArray[index] == ';') {
        String sql = sbLine.substring(beginIndex, index).trim();
        initSqlList.add(sql);
        beginIndex = index + 1;
      }
    }
    return initSqlList;
  }

  private void openTransport() throws Exception {
    transport = isHttpTransportMode() ? createHttpTransport() : createBinaryTransport();
    if (!transport.isOpen()) {
      transport.open();
    }
    logZkDiscoveryMessage("Connected to " + connParams.getHost() + ":" + connParams.getPort());
  }

  public String getConnectedUrl() {
    return jdbcUriString;
  }

  private String getServerHttpUrl(boolean useSsl) {
    // Create the http/https url
    // JDBC driver will set up an https url if ssl is enabled, otherwise http
    String schemeName = useSsl ? "https" : "http";
    // http path should begin with "/"
    String httpPath;
    httpPath = sessConfMap.get(HTTP_PATH);
    if (httpPath == null) {
      httpPath = "/";
    } else if (!httpPath.startsWith("/")) {
      httpPath = "/" + httpPath;
    }
    return schemeName + "://" + host + ":" + port + httpPath;
  }

  private TTransport createHttpTransport() throws SQLException, TTransportException {
    CloseableHttpClient httpClient;
    boolean useSsl = isSslConnection();
    // Create an http client from the configs
    httpClient = getHttpClient(useSsl);
    int maxMessageSize = getMaxMessageSize();
    TConfiguration.Builder tConfBuilder = TConfiguration.custom();
    if (maxMessageSize > 0) {
      tConfBuilder.setMaxMessageSize(maxMessageSize);
    }
    TConfiguration tConf = tConfBuilder.build();
    transport = new THttpClient(tConf, getServerHttpUrl(useSsl), httpClient);
    return transport;
  }

  private CloseableHttpClient getHttpClient(Boolean useSsl) throws SQLException {
    boolean isCookieEnabled = isCookieEnabled();
    String cookieName = sessConfMap.getOrDefault(COOKIE_NAME, DEFAULT_COOKIE_NAMES_HS2);
    CookieStore cookieStore = isCookieEnabled ? new BasicCookieStore() : null;
    // Request interceptor for any request pre-processing logic
    Map<String, String> additionalHttpHeaders = new HashMap<>();
    Map<String, String> customCookies = new HashMap<>();

    // Retrieve the additional HttpHeaders
    for (Map.Entry<String, String> entry : sessConfMap.entrySet()) {
      String key = entry.getKey();

      if (key.startsWith(HTTP_HEADER_PREFIX)) {
        additionalHttpHeaders.put(key.substring(HTTP_HEADER_PREFIX.length()), entry.getValue());
      }
      if (key.startsWith(HTTP_COOKIE_PREFIX)) {
        customCookies.put(key.substring(HTTP_COOKIE_PREFIX.length()), entry.getValue());
      }
    }

    HttpRequestInterceptor requestInterceptor;
    if (!isSaslAuthMode()) {
      requestInterceptor = null;
    } else if (isPlainSaslAuthMode()) {
      if (isJwtAuthMode()) {
        final String signedJwt = getJWT();
        Preconditions.checkArgument(
            signedJwt != null && !signedJwt.isEmpty(),
            "For jwt auth mode," + " a signed jwt must be provided");
        /*
         * Add an interceptor to pass jwt token in the header. In https mode, the entire
         * information is encrypted
         */
        requestInterceptor =
            new HttpJwtAuthRequestInterceptor(
                signedJwt, cookieStore, cookieName, useSsl, additionalHttpHeaders, customCookies);
      } else {
        /*
         * Add an interceptor to pass username/password in the header. In https mode, the entire
         * information is encrypted
         */
        requestInterceptor =
            new HttpBasicAuthInterceptor(
                getUserName(),
                getPassword(),
                cookieStore,
                cookieName,
                useSsl,
                additionalHttpHeaders,
                customCookies);
      }
    } else {
      // Configure http client for kerberos-based authentication
      Subject subject = createSubject();
      /*
       * Add an interceptor which sets the appropriate header in the request. It does the kerberos
       * authentication and get the final service ticket, for sending to the server before every
       * request. In https mode, the entire information is encrypted
       */
      requestInterceptor =
          new HttpKerberosRequestInterceptor(
              sessConfMap.get(AUTH_PRINCIPAL),
              host,
              subject,
              cookieStore,
              cookieName,
              useSsl,
              additionalHttpHeaders,
              customCookies);
    }
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();

    // Set timeout
    RequestConfig config =
        RequestConfig.custom()
            .setConnectTimeout(connectTimeout)
            .setSocketTimeout(socketTimeout)
            .build();
    httpClientBuilder.setDefaultRequestConfig(config);

    // Configure http client for cookie based authentication
    if (isCookieEnabled) {
      // Create a http client with a retry mechanism when the server returns a status code of 401.
      httpClientBuilder.setServiceUnavailableRetryStrategy(
          new ServiceUnavailableRetryStrategy() {
            @Override
            public boolean retryRequest(
                final HttpResponse response, final int executionCount, final HttpContext context) {
              int statusCode = response.getStatusLine().getStatusCode();
              boolean ret = statusCode == 401 && executionCount <= 1;

              // Set the context attribute to true which will be interpreted by the request
              // interceptor
              if (ret) {
                context.setAttribute(HIVE_SERVER2_RETRY_KEY, HIVE_SERVER2_RETRY_TRUE);
              }
              return ret;
            }

            @Override
            public long getRetryInterval() {
              // Immediate retry
              return 0;
            }
          });
    }
    // In case the server's idletimeout is set to a lower value, it might close it's side of
    // connection. However we retry one more time on NoHttpResponseException
    httpClientBuilder.setRetryHandler(
        (exception, executionCount, context) -> {
          if (executionCount > 1) {
            LOG.info("Retry attempts to connect to server exceeded.");
            return false;
          }
          if (exception instanceof NoHttpResponseException) {
            LOG.info("Could not connect to the server. Retrying one more time.");
            return true;
          }
          return false;
        });

    // Add the request interceptor to the client builder
    httpClientBuilder.addInterceptorFirst(requestInterceptor);

    // Add an interceptor to add in an XSRF header
    httpClientBuilder.addInterceptorLast(new HttpXsrfRequestInterceptor());

    // Configure http client for SSL
    if (useSsl) {
      String useTwoWaySSL = sessConfMap.get(USE_TWO_WAY_SSL);
      String sslTrustStorePath = sessConfMap.get(SSL_TRUST_STORE);
      String sslTrustStorePassword =
          Utils.getPassword(sessConfMap, JdbcConnectionParams.SSL_TRUST_STORE_PASSWORD);
      KeyStore sslTrustStore;
      SSLConnectionSocketFactory socketFactory;
      SSLContext sslContext;
      /*
       * The code within the try block throws: SSLInitializationException, KeyStoreException,
       * IOException, NoSuchAlgorithmException, CertificateException, KeyManagementException &
       * UnrecoverableKeyException. We don't want the client to retry on any of these, hence we
       * catch all and throw a SQLException.
       */
      try {
        if (useTwoWaySSL != null && useTwoWaySSL.equalsIgnoreCase(TRUE)) {
          socketFactory = getTwoWaySSLSocketFactory();
        } else if (sslTrustStorePath == null || sslTrustStorePath.isEmpty()) {
          // Create a default socket factory based on standard JSSE trust material
          socketFactory = SSLConnectionSocketFactory.getSocketFactory();
        } else {
          // Pick trust store config from the given path
          sslTrustStore = KeyStore.getInstance(SSL_TRUST_STORE_TYPE);
          try (FileInputStream fis = new FileInputStream(sslTrustStorePath)) {
            sslTrustStore.load(
                fis, sslTrustStorePassword != null ? sslTrustStorePassword.toCharArray() : null);
          }
          sslContext = SSLContexts.custom().loadTrustMaterial(sslTrustStore, null).build();
          socketFactory =
              new SSLConnectionSocketFactory(sslContext, new DefaultHostnameVerifier(null));
        }
        final Registry<ConnectionSocketFactory> registry =
            RegistryBuilder.<ConnectionSocketFactory>create()
                .register("https", socketFactory)
                .build();
        httpClientBuilder.setConnectionManager(new BasicHttpClientConnectionManager(registry));
      } catch (Exception e) {
        String msg =
            "Could not create an https connection to " + jdbcUriString + ". " + e.getMessage();
        throw new KyuubiSQLException(msg, "08S01", e);
      }
    }
    return httpClientBuilder.build();
  }

  private String getJWT() {
    String jwtCredential = getJWTStringFromSession();
    if (jwtCredential == null || jwtCredential.isEmpty()) {
      jwtCredential = getJWTStringFromEnv();
    }
    return jwtCredential;
  }

  private String getJWTStringFromEnv() {
    String jwtCredential = System.getenv(JdbcConnectionParams.AUTH_JWT_ENV);
    if (jwtCredential == null || jwtCredential.isEmpty()) {
      LOG.debug("No JWT is specified in env variable {}", JdbcConnectionParams.AUTH_JWT_ENV);
    } else {
      int startIndex = Math.max(0, jwtCredential.length() - 7);
      String lastSevenChars = jwtCredential.substring(startIndex);
      LOG.debug("Fetched JWT (ends with {}) from the env.", lastSevenChars);
    }
    return jwtCredential;
  }

  private String getJWTStringFromSession() {
    String jwtCredential = sessConfMap.get(JdbcConnectionParams.AUTH_TYPE_JWT_KEY);
    if (jwtCredential == null || jwtCredential.isEmpty()) {
      LOG.debug("No JWT is specified in connection string.");
    } else {
      int startIndex = Math.max(0, jwtCredential.length() - 7);
      String lastSevenChars = jwtCredential.substring(startIndex);
      LOG.debug("Fetched JWT (ends with {}) from the session.", lastSevenChars);
    }
    return jwtCredential;
  }

  /** Create underlying SSL or non-SSL transport */
  private TTransport createUnderlyingTransport() throws TTransportException, SQLException {
    int maxMessageSize = getMaxMessageSize();
    TTransport transport = null;
    // Note: Thrift returns an SSL socket that is already bound to the specified host:port
    // Therefore an open called on this would be a no-op later
    // Hence, any TTransportException related to connecting with the peer are thrown here.
    // Bubbling them up the call hierarchy so that a retry can happen in openTransport,
    // if dynamic service discovery is configured.
    if (isSslConnection()) {
      // get SSL socket
      String sslTrustStore = sessConfMap.get(SSL_TRUST_STORE);
      String sslTrustStorePassword =
          Utils.getPassword(sessConfMap, JdbcConnectionParams.SSL_TRUST_STORE_PASSWORD);

      if (sslTrustStore == null || sslTrustStore.isEmpty()) {
        transport =
            ThriftUtils.getSSLSocket(host, port, connectTimeout, socketTimeout, maxMessageSize);
      } else {
        transport =
            ThriftUtils.getSSLSocket(
                host,
                port,
                connectTimeout,
                socketTimeout,
                sslTrustStore,
                sslTrustStorePassword,
                maxMessageSize);
      }
    } else {
      // get non-SSL socket transport
      transport =
          ThriftUtils.getSocketTransport(host, port, connectTimeout, socketTimeout, maxMessageSize);
    }
    return transport;
  }

  private int getMaxMessageSize() throws SQLException {
    String maxMessageSize = sessConfMap.get(JdbcConnectionParams.THRIFT_CLIENT_MAX_MESSAGE_SIZE);
    if (maxMessageSize == null) {
      return -1;
    }

    try {
      return Integer.parseInt(maxMessageSize);
    } catch (Exception e) {
      String errFormat =
          "Invalid {} configuration of '{}'. Expected an integer specifying number of bytes. "
              + "A configuration of <= 0 uses default max message size.";
      String errMsg =
          String.format(
              errFormat, JdbcConnectionParams.THRIFT_CLIENT_MAX_MESSAGE_SIZE, maxMessageSize);
      throw new SQLException(errMsg, "42000", e);
    }
  }

  /**
   * Create transport per the connection options Supported transport options are: - SASL based
   * transports over + Kerberos + SSL + non-SSL - Raw (non-SASL) socket
   *
   * <p>Kerberos supports SASL QOP configurations
   *
   * @throws SQLException, TTransportException
   */
  private TTransport createBinaryTransport() throws SQLException, TTransportException {
    try {
      TTransport socketTransport = createUnderlyingTransport();
      // Raw socket connection (non-sasl)
      if (!isSaslAuthMode()) {
        return socketTransport;
      }
      // Use PLAIN Sasl connection with user/password
      if (isPlainSaslAuthMode()) {
        String userName = getUserName();
        String passwd = getPassword();
        // Overlay the SASL transport on top of the base socket transport (SSL or non-SSL)
        return PlainSaslHelper.getPlainTransport(userName, passwd, socketTransport);
      }

      // Kerberos enabled
      Map<String, String> saslProps = new HashMap<>();
      saslProps.put(Sasl.SERVER_AUTH, "true");
      // If the client did not specify qop then just negotiate the one supported by server
      saslProps.put(Sasl.QOP, "auth-conf,auth-int,auth");
      if (sessConfMap.containsKey(AUTH_QOP)) {
        try {
          SaslQOP saslQOP = SaslQOP.fromString(sessConfMap.get(AUTH_QOP));
          saslProps.put(Sasl.QOP, saslQOP.toString());
        } catch (IllegalArgumentException e) {
          throw new KyuubiSQLException(
              "Invalid " + AUTH_QOP + " parameter. " + e.getMessage(), "42000", e);
        }
      }

      Subject subject = createSubject();
      String serverPrincipal = sessConfMap.get(AUTH_PRINCIPAL);
      return KerberosSaslHelper.createSubjectAssumedTransport(
          subject, serverPrincipal, host, socketTransport, saslProps);
    } catch (Exception e) {
      throw new KyuubiSQLException(
          "Could not create secure connection to " + jdbcUriString + ": " + e.getMessage(),
          "08S01",
          e);
    }
  }

  SSLConnectionSocketFactory getTwoWaySSLSocketFactory() throws SQLException {
    try {
      KeyManagerFactory keyManagerFactory =
          KeyManagerFactory.getInstance(SUNX509_ALGORITHM_STRING, SUNJSSE_ALGORITHM_STRING);
      String keyStorePath = sessConfMap.get(SSL_KEY_STORE);
      String keyStorePassword =
          Utils.getPassword(sessConfMap, JdbcConnectionParams.SSL_KEY_STORE_PASSWORD);
      KeyStore sslKeyStore = KeyStore.getInstance(SSL_KEY_STORE_TYPE);

      if (keyStorePath == null || keyStorePath.isEmpty()) {
        throw new IllegalArgumentException(
            SSL_KEY_STORE
                + " Not configured for 2 way SSL connection, keyStorePath param is empty");
      }
      try (FileInputStream fis = new FileInputStream(keyStorePath)) {
        sslKeyStore.load(fis, keyStorePassword.toCharArray());
      }
      keyManagerFactory.init(sslKeyStore, keyStorePassword.toCharArray());

      TrustManagerFactory trustManagerFactory =
          TrustManagerFactory.getInstance(SUNX509_ALGORITHM_STRING);
      String trustStorePath = sessConfMap.get(SSL_TRUST_STORE);
      String trustStorePassword =
          Utils.getPassword(sessConfMap, JdbcConnectionParams.SSL_TRUST_STORE_PASSWORD);
      KeyStore sslTrustStore = KeyStore.getInstance(SSL_TRUST_STORE_TYPE);

      if (trustStorePath == null || trustStorePath.isEmpty()) {
        throw new IllegalArgumentException(
            SSL_TRUST_STORE + " Not configured for 2 way SSL connection");
      }
      try (FileInputStream fis = new FileInputStream(trustStorePath)) {
        sslTrustStore.load(
            fis, trustStorePassword != null ? trustStorePassword.toCharArray() : null);
      }
      trustManagerFactory.init(sslTrustStore);
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(
          keyManagerFactory.getKeyManagers(),
          trustManagerFactory.getTrustManagers(),
          new SecureRandom());
      return new SSLConnectionSocketFactory(context);
    } catch (Exception e) {
      throw new KyuubiSQLException("Error while initializing 2 way ssl socket factory ", e);
    }
  }

  private void openSession() throws SQLException {
    TOpenSessionReq openReq = new TOpenSessionReq();

    Map<String, String> openConf = new HashMap<>();
    // for remote JDBC client, try to set the conf var using 'set foo=bar'
    for (Entry<String, String> hiveConf : connParams.getHiveConfs().entrySet()) {
      openConf.put("set:hiveconf:" + hiveConf.getKey(), hiveConf.getValue());
    }
    // For remote JDBC client, try to set the hive var using 'set hivevar:key=value'
    for (Entry<String, String> hiveVar : connParams.getHiveVars().entrySet()) {
      openConf.put("set:hivevar:" + hiveVar.getKey(), hiveVar.getValue());
    }
    // switch the catalog
    if (connParams.getCatalogName() != null) {
      openConf.put("use:catalog", connParams.getCatalogName());
    }
    // switch the database
    openConf.put("use:database", connParams.getDbName());
    if (wmPool != null) {
      openConf.put("set:hivevar:wmpool", wmPool);
    }
    if (wmApp != null) {
      openConf.put("set:hivevar:wmapp", wmApp);
    }

    // set the session configuration
    Map<String, String> sessVars = connParams.getSessionVars();
    if (sessVars.containsKey(HS2_PROXY_USER)) {
      openConf.put(HS2_PROXY_USER, sessVars.get(HS2_PROXY_USER));
    }
    String clientProtocolStr =
        sessVars.getOrDefault(
            CLIENT_PROTOCOL_VERSION, openReq.getClient_protocol().getValue() + "");
    TProtocolVersion clientProtocol =
        TProtocolVersion.findByValue(Integer.parseInt(clientProtocolStr));
    if (clientProtocol == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported Hive2 protocol version %s specified by session conf key %s",
              clientProtocolStr, CLIENT_PROTOCOL_VERSION));
    }
    openReq.setClient_protocol(clientProtocol);
    // HIVE-14901: set the fetchSize
    if (clientProtocol.compareTo(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10) >= 0) {
      openConf.put(
          "set:hiveconf:hive.server2.thrift.resultset.default.fetch.size",
          Integer.toString(fetchSize));
    }
    try {
      openConf.put("kyuubi.client.ipAddress", InetAddress.getLocalHost().getHostAddress());
    } catch (UnknownHostException e) {
      LOG.debug("Error getting Kyuubi session local client ip address", e);
    }
    openConf.put(Utils.KYUUBI_CLIENT_VERSION_KEY, Utils.getVersion());
    openReq.setConfiguration(openConf);

    // Store the user name in the open request in case no non-sasl authentication
    if (AUTH_SIMPLE.equals(sessConfMap.get(AUTH_TYPE))) {
      openReq.setUsername(sessConfMap.get(AUTH_USER));
      openReq.setPassword(sessConfMap.get(AUTH_PASSWD));
    }

    try {
      TOpenSessionResp openResp = client.OpenSession(openReq);

      // validate connection
      Utils.verifySuccess(openResp.getStatus());
      if (!supportedProtocols.contains(openResp.getServerProtocolVersion())) {
        throw new TException("Unsupported Hive2 protocol");
      }
      protocol = openResp.getServerProtocolVersion();
      sessHandle = openResp.getSessionHandle();

      Map<String, String> openRespConf = openResp.getConfiguration();
      // Update fetchSize if modified by server
      String serverFetchSize = openRespConf.get("hive.server2.thrift.resultset.default.fetch.size");
      if (serverFetchSize != null) {
        fetchSize = Integer.parseInt(serverFetchSize);
      }

      // Get launch engine operation handle
      String launchEngineOpHandleGuid =
          openRespConf.get("kyuubi.session.engine.launch.handle.guid");
      String launchEngineOpHandleSecret =
          openRespConf.get("kyuubi.session.engine.launch.handle.secret");

      launchEngineOpSupportResult =
          Boolean.parseBoolean(
              openRespConf.getOrDefault("kyuubi.session.engine.launch.support.result", "false"));

      if (launchEngineOpHandleGuid != null && launchEngineOpHandleSecret != null) {
        try {
          byte[] guidBytes = Base64.getDecoder().decode(launchEngineOpHandleGuid);
          byte[] secretBytes = Base64.getDecoder().decode(launchEngineOpHandleSecret);
          THandleIdentifier handleIdentifier =
              new THandleIdentifier(ByteBuffer.wrap(guidBytes), ByteBuffer.wrap(secretBytes));
          launchEngineOpHandle =
              new TOperationHandle(handleIdentifier, TOperationType.UNKNOWN, false);
        } catch (Exception e) {
          LOG.error("Failed to decode launch engine operation handle from open session resp", e);
        }
      }
    } catch (TException e) {
      LOG.error("Error opening session", e);
      throw new KyuubiSQLException(
          "Could not establish connection to " + jdbcUriString + ": " + e.getMessage(), "08S01", e);
    }
    isClosed = false;
  }

  /** @return username from sessConfMap */
  private String getUserName() {
    return getSessionValue(AUTH_USER, ANONYMOUS_USER);
  }

  /** @return password from sessConfMap */
  private String getPassword() {
    return getSessionValue(AUTH_PASSWD, ANONYMOUS_PASSWD);
  }

  private boolean isCookieEnabled() {
    return !"false".equalsIgnoreCase(sessConfMap.get(COOKIE_AUTH));
  }

  private boolean isSslConnection() {
    return "true".equalsIgnoreCase(sessConfMap.get(USE_SSL));
  }

  private boolean isSaslAuthMode() {
    return !AUTH_SIMPLE.equalsIgnoreCase(sessConfMap.get(AUTH_TYPE));
  }

  private boolean isHadoopUserGroupInformationDoAs() {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends Principal> HadoopUserClz =
          (Class<? extends Principal>) ClassUtils.getClass("org.apache.hadoop.security.User");
      Subject subject = Subject.getSubject(AccessController.getContext());
      return subject != null && !subject.getPrincipals(HadoopUserClz).isEmpty();
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  private boolean isForciblyFromKeytabAuthMode() {
    return AUTH_KERBEROS_AUTH_TYPE_FROM_KEYTAB.equalsIgnoreCase(
        sessConfMap.get(AUTH_KERBEROS_AUTH_TYPE));
  }

  private boolean isForciblyFromSubjectAuthMode() {
    return AUTH_KERBEROS_AUTH_TYPE_FROM_SUBJECT.equalsIgnoreCase(
        sessConfMap.get(AUTH_KERBEROS_AUTH_TYPE));
  }

  private boolean isForciblyTgtCacheAuthMode() {
    return AUTH_KERBEROS_AUTH_TYPE_FROM_TICKET_CACHE.equalsIgnoreCase(
        sessConfMap.get(AUTH_KERBEROS_AUTH_TYPE));
  }

  private boolean isKeytabAuthMode() {
    // handle explicit cases first
    if (isForciblyFromSubjectAuthMode() || isForciblyTgtCacheAuthMode()) {
      return false;
    }
    if (isKerberosAuthMode() && isForciblyFromKeytabAuthMode()) {
      return true;
    }
    if (isKerberosAuthMode()
        && hasSessionValue(AUTH_KYUUBI_CLIENT_KEYTAB)
        && !hasSessionValue(AUTH_KYUUBI_CLIENT_PRINCIPAL)) {
      throw new IllegalArgumentException(
          AUTH_KYUUBI_CLIENT_KEYTAB
              + " is set but "
              + AUTH_KYUUBI_CLIENT_PRINCIPAL
              + " is not set");
    }
    // handle implicit cases then
    return isKerberosAuthMode()
        && hasSessionValue(AUTH_KYUUBI_CLIENT_PRINCIPAL)
        && hasSessionValue(AUTH_KYUUBI_CLIENT_KEYTAB);
  }

  private boolean isFromSubjectAuthMode() {
    // handle explicit cases first
    if (isForciblyFromKeytabAuthMode() || isForciblyTgtCacheAuthMode()) {
      return false;
    }
    if (isKerberosAuthMode() && isForciblyFromSubjectAuthMode()) {
      return true;
    }
    // handle implicit cases then
    return isKerberosAuthMode()
        && !hasSessionValue(AUTH_KYUUBI_CLIENT_KEYTAB)
        && isHadoopUserGroupInformationDoAs();
  }

  private boolean isTgtCacheAuthMode() {
    // handle explicit cases first
    if (isForciblyFromKeytabAuthMode() || isForciblyFromSubjectAuthMode()) {
      return false;
    }
    if (isKerberosAuthMode() && isForciblyTgtCacheAuthMode()) {
      return true;
    }
    // handle implicit cases then
    return isKerberosAuthMode() && !hasSessionValue(AUTH_KYUUBI_CLIENT_KEYTAB);
  }

  private boolean isPlainSaslAuthMode() {
    return isSaslAuthMode() && !hasSessionValue(AUTH_PRINCIPAL);
  }

  private boolean isJwtAuthMode() {
    return JdbcConnectionParams.AUTH_TYPE_JWT.equalsIgnoreCase(
            sessConfMap.get(JdbcConnectionParams.AUTH_TYPE))
        || sessConfMap.containsKey(JdbcConnectionParams.AUTH_TYPE_JWT_KEY);
  }

  private boolean isKerberosAuthMode() {
    return isSaslAuthMode() && hasSessionValue(AUTH_PRINCIPAL);
  }

  private Subject createSubject() {
    if (isKeytabAuthMode()) {
      String principal = sessConfMap.get(AUTH_KYUUBI_CLIENT_PRINCIPAL);
      String keytab = sessConfMap.get(AUTH_KYUUBI_CLIENT_KEYTAB);
      return KerberosAuthenticationManager.getKeytabAuthentication(principal, keytab).getSubject();
    } else if (isFromSubjectAuthMode()) {
      AccessControlContext context = AccessController.getContext();
      return Subject.getSubject(context);
    } else if (isTgtCacheAuthMode()) {
      String ticketCache = sessConfMap.getOrDefault(AUTH_KYUUBI_CLIENT_TICKET_CACHE, "");
      return KerberosAuthenticationManager.getTgtCacheAuthentication(ticketCache).getSubject();
    } else {
      // This should never happen
      throw new IllegalArgumentException("Unsupported auth mode");
    }
  }

  private boolean isHttpTransportMode() {
    return "http".equalsIgnoreCase(sessConfMap.get(TRANSPORT_MODE));
  }

  private void logZkDiscoveryMessage(String message) {
    if (ZooKeeperHiveClientHelper.isZkDynamicDiscoveryMode(sessConfMap)) {
      LOG.info(message);
    }
  }

  private boolean hasSessionValue(String varName) {
    String varValue = sessConfMap.get(varName);
    return !(varValue == null || varValue.isEmpty());
  }

  /** Lookup varName in sessConfMap, if its null or empty return the default value varDefault */
  private String getSessionValue(String varName, String varDefault) {
    String varValue = sessConfMap.get(varName);
    if (varValue == null || varValue.isEmpty()) {
      varValue = varDefault;
    }
    return varValue;
  }

  private void setupTimeout() {
    if (sessConfMap.containsKey(CONNECT_TIMEOUT)) {
      String connectTimeoutStr = sessConfMap.get(CONNECT_TIMEOUT);
      try {
        long connectTimeoutMs = Long.parseLong(connectTimeoutStr);
        connectTimeout = (int) Math.max(0, Math.min(connectTimeoutMs, Integer.MAX_VALUE));
      } catch (NumberFormatException e) {
        LOG.info("Failed to parse connectTimeout of value " + connectTimeoutStr);
      }
    }
    if (sessConfMap.containsKey(SOCKET_TIMEOUT)) {
      String socketTimeoutStr = sessConfMap.get(SOCKET_TIMEOUT);
      try {
        long socketTimeoutMs = Long.parseLong(socketTimeoutStr);
        socketTimeout = (int) Math.max(0, Math.min(socketTimeoutMs, Integer.MAX_VALUE));
      } catch (NumberFormatException e) {
        LOG.info("Failed to parse socketTimeout of value " + socketTimeoutStr);
      }
    }
  }

  public String getDelegationToken(String owner, String renewer) throws SQLException {
    if (isClosed) {
      throw new KyuubiSQLException("Connection is closed");
    }
    TGetDelegationTokenReq req = new TGetDelegationTokenReq(sessHandle, owner, renewer);
    try {
      TGetDelegationTokenResp tokenResp = client.GetDelegationToken(req);
      Utils.verifySuccess(tokenResp.getStatus());
      return tokenResp.getDelegationToken();
    } catch (TException e) {
      throw new KyuubiSQLException("Could not retrieve token: " + e.getMessage(), "08S01", e);
    }
  }

  public void cancelDelegationToken(String tokenStr) throws SQLException {
    if (isClosed) {
      throw new KyuubiSQLException("Connection is closed");
    }
    TCancelDelegationTokenReq cancelReq = new TCancelDelegationTokenReq(sessHandle, tokenStr);
    try {
      TCancelDelegationTokenResp cancelResp = client.CancelDelegationToken(cancelReq);
      Utils.verifySuccess(cancelResp.getStatus());
    } catch (TException e) {
      throw new KyuubiSQLException("Could not cancel token: " + e.getMessage(), "08S01", e);
    }
  }

  public void renewDelegationToken(String tokenStr) throws SQLException {
    if (isClosed) {
      throw new KyuubiSQLException("Connection is closed");
    }
    TRenewDelegationTokenReq cancelReq = new TRenewDelegationTokenReq(sessHandle, tokenStr);
    try {
      TRenewDelegationTokenResp renewResp = client.RenewDelegationToken(cancelReq);
      Utils.verifySuccess(renewResp.getStatus());
    } catch (TException e) {
      throw new KyuubiSQLException("Could not renew token: " + e.getMessage(), "08S01", e);
    }
  }

  @Override
  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  @Override
  public void close() throws SQLException {
    try {
      if (!isClosed) {
        TCloseSessionReq closeReq = new TCloseSessionReq(sessHandle);
        client.CloseSession(closeReq);
      }
    } catch (TException e) {
      throw new KyuubiSQLException("Error while cleaning up the server resources", e);
    } finally {
      isClosed = true;
      client = null;
      if (transport != null && transport.isOpen()) {
        transport.close();
        transport = null;
      }
    }
  }

  private void closeOnLaunchEngineFailure() throws SQLException {
    if (engineLogThread != null && engineLogThread.isAlive()) {
      engineLogThread.interrupt();
      try {
        engineLogThread.join(DEFAULT_ENGINE_LOG_THREAD_TIMEOUT);
      } catch (Exception ignore) {
      }
    }
    engineLogThread = null;
    close();
  }

  /**
   * Creates a Statement object for sending SQL statements to the database.
   *
   * @throws SQLException if a database access error occurs.
   * @see java.sql.Connection#createStatement()
   */
  @Override
  public Statement createStatement() throws SQLException {
    if (isClosed) {
      throw new KyuubiSQLException("Can't create Statement, connection is closed");
    }
    return new KyuubiStatement(this, client, sessHandle, fetchSize);
  }

  private KyuubiStatement createKyuubiStatement() throws SQLException {
    return ((KyuubiStatement) createStatement());
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new KyuubiSQLException(
          "Statement with resultset concurrency " + resultSetConcurrency + " is not supported",
          "HYC00"); // Optional feature not implemented
    }
    if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
      throw new KyuubiSQLException(
          "Statement with resultset type " + resultSetType + " is not supported",
          "HYC00"); // Optional feature not implemented
    }
    if (isClosed) {
      throw new KyuubiSQLException("Connection is closed");
    }
    return new KyuubiStatement(
        this, client, sessHandle, resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE, fetchSize);
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return true;
  }

  @Override
  public String getCatalog() throws SQLException {
    if (isClosed) {
      throw new KyuubiSQLException("Connection is closed");
    }
    try (KyuubiStatement stmt = createKyuubiStatement();
        ResultSet res = stmt.executeGetCurrentCatalog("_GET_CATALOG")) {
      if (!res.next()) {
        throw new KyuubiSQLException("Failed to get catalog information");
      }
      return res.getString(1);
    } catch (Exception ignore) {
      return "";
    }
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return clientInfo == null ? new Properties() : clientInfo;
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    if (clientInfo == null) return null;
    return clientInfo.getProperty(name);
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    if (isClosed) {
      throw new KyuubiSQLException("Connection is closed");
    }
    return new KyuubiDatabaseMetaData(this, protocol, client, sessHandle);
  }

  @Override
  public String getSchema() throws SQLException {
    if (isClosed) {
      throw new KyuubiSQLException("Connection is closed");
    }
    try (KyuubiStatement stmt = createKyuubiStatement();
        ResultSet res = stmt.executeGetCurrentDatabase("SELECT current_database()")) {
      if (!res.next()) {
        throw new KyuubiSQLException("Failed to get schema information");
      }
      return res.getString(1);
    }
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    if (timeout < 0) {
      throw new KyuubiSQLException("timeout value was negative");
    }
    if (isClosed) {
      return false;
    }
    boolean rc = false;
    try {
      new KyuubiDatabaseMetaData(this, protocol, client, sessHandle).getDatabaseProductName();
      rc = true;
    } catch (SQLException ignore) {
    }
    return rc;
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    if (isClosed) {
      throw new KyuubiSQLException("Connection is closed");
    }
    return new KyuubiPreparedStatement(this, client, sessHandle, sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    if (isClosed) {
      throw new KyuubiSQLException("Connection is closed");
    }
    return new KyuubiPreparedStatement(this, client, sessHandle, sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    if (isClosed) {
      throw new KyuubiSQLException("Connection is closed");
    }
    return new KyuubiPreparedStatement(this, client, sessHandle, sql);
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    // Per JDBC spec, if the connection is closed a SQLException should be thrown.
    if (isClosed) {
      throw new KyuubiSQLException("Connection is closed");
    }
    // The auto-commit mode is always enabled for this connection. Per JDBC spec,
    // if setAutoCommit is called and the auto-commit mode is not changed, the call is a no-op.
    if (!autoCommit) {
      LOG.warn("Request to set autoCommit to false; Hive does not support autoCommit=false.");
      SQLWarning warning = new SQLWarning("Hive does not support autoCommit=false");
      if (warningChain == null) warningChain = warning;
      else warningChain.setNextWarning(warning);
    }
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    if (isClosed) {
      throw new KyuubiSQLException("Connection is closed");
    }
    try (KyuubiStatement stmt = createKyuubiStatement()) {
      stmt.executeSetCurrentCatalog("_SET_CATALOG", catalog);
    } catch (SQLException ignore) {
    }
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    clientInfo = properties;
    setClientInfo();
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    if (clientInfo == null) {
      clientInfo = new Properties();
    }
    clientInfo.put(name, value);
    setClientInfo();
  }

  private void setClientInfo() throws SQLClientInfoException {
    if (isClosed) {
      throw new SQLClientInfoException("Connection is closed", null);
    }
    TSetClientInfoReq req = new TSetClientInfoReq(sessHandle);
    Map<String, String> map = new HashMap<>();
    if (clientInfo != null) {
      for (Entry<Object, Object> e : clientInfo.entrySet()) {
        if (e.getKey() == null || e.getValue() == null) continue;
        map.put(e.getKey().toString(), e.getValue().toString());
      }
    }
    req.setConfiguration(map);
    try {
      TSetClientInfoResp openResp = client.SetClientInfo(req);
      Utils.verifySuccess(openResp.getStatus());
    } catch (TException | SQLException e) {
      LOG.error("Error setting client info", e);
      throw new SQLClientInfoException("Error setting client info", null, e);
    }
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    // Per JDBC spec, if the connection is closed a SQLException should be thrown.
    if (isClosed) {
      throw new KyuubiSQLException("Connection is closed");
    }
    // Per JDBC spec, the request defines a hint to the driver to enable database optimizations.
    // The read-only mode for this connection is disabled and cannot be enabled (isReadOnly always
    // returns false).
    // The most correct behavior is to throw only if the request tries to enable the read-only mode.
    if (readOnly) {
      throw new KyuubiSQLException("Enabling read-only mode not supported");
    }
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    // JDK 1.7
    if (isClosed) {
      throw new KyuubiSQLException("Connection is closed");
    }
    if (schema == null || schema.isEmpty()) {
      throw new KyuubiSQLException("Schema name is null or empty");
    }
    try (KyuubiStatement stmt = createKyuubiStatement()) {
      stmt.executeSetCurrentDatabase("use " + schema, schema);
    }
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    // TODO: throw an exception?
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (!isWrapperFor(iface)) {
      throw new KyuubiSQLException(
          this.getClass().getName() + " not unwrappable from " + iface.getName());
    }
    return iface.cast(this);
  }

  public TProtocolVersion getProtocol() {
    return protocol;
  }

  @SuppressWarnings("rawtypes")
  public static TCLIService.Iface newSynchronizedClient(TCLIService.Iface client) {
    return (TCLIService.Iface)
        Proxy.newProxyInstance(
            KyuubiConnection.class.getClassLoader(),
            new Class[] {TCLIService.Iface.class},
            new SynchronizedHandler(client));
  }

  private static class SynchronizedHandler implements InvocationHandler {
    private final TCLIService.Iface client;
    private final ReentrantLock lock = new ReentrantLock(true);

    SynchronizedHandler(TCLIService.Iface client) {
      this.client = client;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      try {
        lock.lock();
        return method.invoke(client, args);
      } catch (InvocationTargetException e) {
        // all IFace APIs throw TException
        if (e.getTargetException() instanceof TException) {
          throw e.getTargetException();
        } else {
          // should not happen
          throw new TException(
              "Error in calling method " + method.getName(), e.getTargetException());
        }
      } catch (Exception e) {
        throw new TException("Error in calling method " + method.getName(), e);
      } finally {
        lock.unlock();
      }
    }
  }

  @SuppressWarnings("fallthrough")
  public void waitLaunchEngineToComplete() throws SQLException {
    if (launchEngineOpHandle == null) return;

    TGetOperationStatusReq statusReq = new TGetOperationStatusReq(launchEngineOpHandle);

    // Poll on the operation status, till the operation is complete
    while (!launchEngineOpCompleted) {
      try {
        TGetOperationStatusResp statusResp = client.GetOperationStatus(statusReq);
        Utils.verifySuccessWithInfo(statusResp.getStatus());
        if (statusResp.isSetOperationState()) {
          switch (statusResp.getOperationState()) {
            case FINISHED_STATE:
              fetchLaunchEngineResult();
            case CLOSED_STATE:
              launchEngineOpCompleted = true;
              engineLogInflight = false;
              break;
            case CANCELED_STATE:
              // 01000 -> warning
              throw new KyuubiSQLException("Launch engine was cancelled", "01000");
            case TIMEDOUT_STATE:
              throw new SQLTimeoutException("Launch engine timeout");
            case ERROR_STATE:
              // Get the error details from the underlying exception
              throw new KyuubiSQLException(
                  statusResp.getErrorMessage(),
                  statusResp.getSqlState(),
                  statusResp.getErrorCode());
            case UKNOWN_STATE:
              throw new KyuubiSQLException("Unknown state", "HY000");
            case INITIALIZED_STATE:
            case PENDING_STATE:
            case RUNNING_STATE:
              break;
          }
        }
      } catch (Exception e) {
        engineLogInflight = false;
        closeOnLaunchEngineFailure();
        if (e instanceof SQLException) {
          throw (SQLException) e;
        } else {
          throw new KyuubiSQLException(e.getMessage(), "08S01", e);
        }
      }
    }
  }

  private void fetchLaunchEngineResult() {
    if (launchEngineOpHandle == null || !launchEngineOpSupportResult) return;

    TFetchResultsReq tFetchResultsReq =
        new TFetchResultsReq(
            launchEngineOpHandle, TFetchOrientation.FETCH_NEXT, KyuubiStatement.DEFAULT_FETCH_SIZE);

    try {
      TFetchResultsResp tFetchResultsResp = client.FetchResults(tFetchResultsReq);
      RowSet rowSet = RowSetFactory.create(tFetchResultsResp.getResults(), this.getProtocol());
      for (Object[] row : rowSet) {
        String key = String.valueOf(row[0]);
        String value = String.valueOf(row[1]);
        if ("id".equals(key)) {
          engineId = value;
        } else if ("name".equals(key)) {
          engineName = value;
        } else if ("url".equals(key)) {
          engineUrl = value;
        } else if ("refId".equals(key)) {
          engineRefId = value;
        }
      }
    } catch (Exception e) {
      LOG.error("Error fetching launch engine result", e);
    }
  }

  public String getEngineId() {
    return engineId;
  }

  public String getEngineName() {
    return engineName;
  }

  public String getEngineUrl() {
    return engineUrl;
  }

  public String getEngineRefId() {
    return engineRefId;
  }
}
