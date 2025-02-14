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

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TStatus;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TStatusCode;
import org.apache.kyuubi.util.reflect.DynConstructors;
import org.apache.kyuubi.util.reflect.DynMethods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
  static final Logger LOG = LoggerFactory.getLogger(Utils.class);
  /** The required prefix list for the connection URL. */
  public static final List<String> URL_PREFIX_LIST =
      Arrays.asList("jdbc:hive2://", "jdbc:kyuubi://");

  /** If host is provided, without a port. */
  static final String DEFAULT_PORT = "10009";
  // To parse the intermediate URI as a Java URI, we'll give a dummy authority(dummyhost:00000).
  // Later, we'll substitute the dummy authority for a resolved authority.
  static final String dummyAuthorityString = "dummyhost:00000";

  /** Kyuubi's default database name */
  static final String DEFAULT_DATABASE = "default";

  private static final String URI_JDBC_PREFIX = "jdbc:";

  private static final String URI_HIVE_PREFIX = "hive2:";

  // This value is set to true by the setServiceUnavailableRetryStrategy() when the server returns
  // 401
  public static final String HIVE_SERVER2_RETRY_KEY = "hive.server2.retryserver";
  public static final String HIVE_SERVER2_RETRY_TRUE = "true";
  public static final String HIVE_SERVER2_RETRY_FALSE = "false";

  public static final String HADOOP_CONFIGURATION_CLASS = "org.apache.hadoop.conf.Configuration";

  public static final String HADOOP_SECURITY_CREDENTIAL_PATH =
      "hadoop.security.credential.provider.path";

  public static final Pattern KYUUBI_OPERATION_HINT_PATTERN =
      Pattern.compile("^__kyuubi_operation_result_(.*)__=(.*)", Pattern.CASE_INSENSITIVE);

  public static final String CLIENT_IP_ADDRESS;

  static {
    String localIpAddress;
    try {
      localIpAddress = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      localIpAddress = "unknown";
      LOG.warn("Error getting Kyuubi local client ip address", e);
    }
    CLIENT_IP_ADDRESS = localIpAddress;
  }

  static String getMatchedUrlPrefix(String uri) throws JdbcUriParseException {
    for (String urlPrefix : URL_PREFIX_LIST) {
      if (uri.startsWith(urlPrefix)) {
        return urlPrefix;
      }
    }
    throw new JdbcUriParseException(
        "Bad URL format: Missing prefix " + String.join(" or ", URL_PREFIX_LIST));
  }

  // Verify success or success_with_info status, else throw SQLException
  static void verifySuccessWithInfo(TStatus status) throws SQLException {
    verifySuccess(status, true);
  }

  // Verify success status, else throw SQLException
  static void verifySuccess(TStatus status) throws SQLException {
    verifySuccess(status, false);
  }

  // Verify success and optionally with_info status, else throw SQLException
  static void verifySuccess(TStatus status, boolean withInfo) throws SQLException {
    if (status.getStatusCode() == TStatusCode.SUCCESS_STATUS
        || (withInfo && status.getStatusCode() == TStatusCode.SUCCESS_WITH_INFO_STATUS)) {
      return;
    }
    throw new KyuubiSQLException(status);
  }

  /**
   * Splits the parametered sql statement at parameter boundaries.
   *
   * <p>taking into account ' and \ escaping.
   *
   * <p>output for: 'select 1 from ? where a = ?' ['select 1 from ',' where a = ','']
   */
  static List<String> splitSqlStatement(String sql) {
    List<String> parts = new ArrayList<>();
    boolean inSingleQuote = false;
    boolean inDoubleQuote = false;
    boolean inComment = false;
    int off = 0;
    boolean skip = false;

    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (inComment) {
        inComment = (c != '\n');
        continue;
      }
      if (skip) {
        skip = false;
        continue;
      }
      switch (c) {
        case '\'':
          if (!inDoubleQuote) {
            inSingleQuote = !inSingleQuote;
          }
          break;
        case '\"':
          if (!inSingleQuote) {
            inDoubleQuote = !inDoubleQuote;
          }
          break;
        case '-':
          if (!inSingleQuote && !inDoubleQuote) {
            if (i < sql.length() - 1 && sql.charAt(i + 1) == '-') {
              inComment = true;
            }
          }
          break;
        case '\\':
          if (!inSingleQuote && !inDoubleQuote) {
            skip = true;
          }
          break;
        case '?':
          if (!inSingleQuote && !inDoubleQuote) {
            parts.add(sql.substring(off, i));
            off = i + 1;
          }
          break;
        default:
          break;
      }
    }
    parts.add(sql.substring(off));
    return parts;
  }

  /** update the SQL string with parameters set by setXXX methods of {@link PreparedStatement} */
  public static String updateSql(final String sql, HashMap<Integer, String> parameters)
      throws SQLException {
    List<String> parts = splitSqlStatement(sql);

    StringBuilder newSql = new StringBuilder(parts.get(0));
    for (int i = 1; i < parts.size(); i++) {
      if (!parameters.containsKey(i)) {
        throw new KyuubiSQLException("Parameter #" + i + " is unset");
      }
      newSql.append(parameters.get(i));
      newSql.append(parts.get(i));
    }
    return newSql.toString();
  }

  public static JdbcConnectionParams parseURL(String uri)
      throws JdbcUriParseException, SQLException, ZooKeeperHiveClientException {
    return parseURL(uri, new Properties());
  }

  /**
   * Parse JDBC connection URL The new format of the URL is:
   * jdbc:hive2://<host1>:<port1>,<host2>:<port2>/dbName;sess_var_list?hive_conf_list#hive_var_list
   * where the optional sess, conf and var lists are semicolon separated <key>=<val> pairs. For
   * utilizing dynamic service discovery with HiveServer2 multiple comma separated host:port pairs
   * can be specified as shown above. The JDBC driver resolves the list of uris and picks a specific
   * server instance to connect to. Currently, dynamic service discovery using ZooKeeper is
   * supported, in which case the host:port pairs represent a ZooKeeper ensemble.
   *
   * <p>As before, if the host/port is not specified, it the driver runs an embedded hive. examples
   * -
   * jdbc:hive2://ubuntu:11000/db2?hive.cli.conf.printheader=true;hive.exec.mode.local.auto.inputbytes.max=9999#stab=salesTable;icol=customerID
   * jdbc:hive2://?hive.cli.conf.printheader=true;hive.exec.mode.local.auto.inputbytes.max=9999#stab=salesTable;icol=customerID
   * jdbc:hive2://ubuntu:11000/db2;user=foo;password=bar
   *
   * <p>Connect to http://server:10001/hs2, with specified basicAuth credentials and initial
   * database:
   * jdbc:hive2://server:10001/db;user=foo;password=bar?hive.server2.transport.mode=http;hive.server2.thrift.http.path=hs2
   */
  public static JdbcConnectionParams parseURL(String uri, Properties info)
      throws JdbcUriParseException, SQLException, ZooKeeperHiveClientException {
    JdbcConnectionParams connParams = extractURLComponents(uri, info);
    if (ZooKeeperHiveClientHelper.isZkDynamicDiscoveryMode(connParams.getSessionVars())) {
      configureConnParamsFromZooKeeper(connParams);
    }
    handleAllDeprecations(connParams);
    return connParams;
  }

  /**
   * This method handles the base parsing of the given jdbc uri. Some of JdbcConnectionParams
   * returned from this method are updated if ZooKeeper is used for service discovery
   */
  public static JdbcConnectionParams extractURLComponents(String uri, Properties info)
      throws JdbcUriParseException {
    JdbcConnectionParams connParams = new JdbcConnectionParams();

    // The JDBC URI now supports specifying multiple host:port if dynamic service discovery is
    // configured on HiveServer2 (like: host1:port1,host2:port2,host3:port3)
    // We'll extract the authorities (host:port combo) from the URI, extract session vars, hive
    // confs & hive vars by parsing it as a Java URI.
    String authorityFromClientJdbcURL = getAuthorityFromJdbcURL(uri);
    if (authorityFromClientJdbcURL.isEmpty()) {
      // Given uri of the form:
      // jdbc:hive2:///dbName;sess_var_list?hive_conf_list#hive_var_list
      authorityFromClientJdbcURL = "localhost:10009";
      String urlPrefix = getMatchedUrlPrefix(uri);
      uri = uri.replace(urlPrefix, urlPrefix + authorityFromClientJdbcURL);
    }
    connParams.setSuppliedURLAuthority(authorityFromClientJdbcURL);
    uri = uri.replaceFirst(authorityFromClientJdbcURL, dummyAuthorityString);

    // Now parse the connection uri with dummy authority
    URI jdbcURI = URI.create(uri.substring(URI_JDBC_PREFIX.length()));

    // key=value pattern
    Pattern pattern = Pattern.compile("([^;]*)=([^;]*);?");

    // dbname and session settings
    String sessVars = jdbcURI.getPath();
    if ((sessVars != null) && !sessVars.isEmpty()) {
      String dbName = "";
      String catalogName = "";
      // removing leading '/' returned by getPath()
      sessVars = sessVars.substring(1);
      if (!sessVars.contains(";")) {
        if (sessVars.contains("/")) {
          catalogName = sessVars.substring(0, sessVars.indexOf('/'));
          dbName = sessVars.substring(sessVars.indexOf('/') + 1);
        } else {
          // only dbname is provided
          dbName = sessVars;
        }
      } else {
        // we have dbname followed by session parameters
        String catalogAndDb = sessVars.substring(0, sessVars.indexOf(';'));
        if (catalogAndDb.contains("/")) {
          catalogName = catalogAndDb.substring(0, catalogAndDb.indexOf('/'));
          dbName = catalogAndDb.substring(catalogAndDb.indexOf('/') + 1);
        } else {
          // only dbname is provided
          dbName = catalogAndDb;
        }
        sessVars = sessVars.substring(sessVars.indexOf(';') + 1);
        Matcher sessMatcher = pattern.matcher(sessVars);
        while (sessMatcher.find()) {
          if (connParams.getSessionVars().put(sessMatcher.group(1), sessMatcher.group(2)) != null) {
            throw new JdbcUriParseException(
                "Bad URL format: Multiple values for property " + sessMatcher.group(1));
          }
        }
      }
      if (!catalogName.isEmpty()) {
        connParams.setCatalogName(catalogName);
      }
      if (!dbName.isEmpty()) {
        connParams.setDbName(dbName);
      }
    }

    Pattern confPattern = Pattern.compile("([^;]*)([^;]*);?");

    // parse hive conf settings
    String confStr = jdbcURI.getQuery();
    if (confStr != null) {
      Matcher confMatcher = confPattern.matcher(confStr);
      while (confMatcher.find()) {
        String connParam = confMatcher.group(1);
        if (StringUtils.isNotBlank(connParam) && connParam.contains("=")) {
          int symbolIndex = connParam.indexOf('=');
          connParams
              .getHiveConfs()
              .put(connParam.substring(0, symbolIndex), connParam.substring(symbolIndex + 1));
        }
      }
    }

    // parse hive var settings
    String varStr = jdbcURI.getFragment();
    if (varStr != null) {
      Matcher varMatcher = pattern.matcher(varStr);
      while (varMatcher.find()) {
        connParams.getHiveVars().put(varMatcher.group(1), varMatcher.group(2));
      }
    }

    // Apply configs supplied in the JDBC connection properties object
    for (Map.Entry<Object, Object> kv : info.entrySet()) {
      if ((kv.getKey() instanceof String)) {
        String key = (String) kv.getKey();
        if (key.startsWith(HIVE_VAR_PREFIX)) {
          connParams
              .getHiveVars()
              .put(key.substring(HIVE_VAR_PREFIX.length()), info.getProperty(key));
        } else if (key.startsWith(HIVE_CONF_PREFIX)) {
          connParams
              .getHiveConfs()
              .put(key.substring(HIVE_CONF_PREFIX.length()), info.getProperty(key));
        }
      }
    }
    if (!connParams.getSessionVars().containsKey(CLIENT_PROTOCOL_VERSION)) {
      if (info.containsKey(CLIENT_PROTOCOL_VERSION)) {
        connParams
            .getSessionVars()
            .put(CLIENT_PROTOCOL_VERSION, info.getProperty(CLIENT_PROTOCOL_VERSION));
      }
    }
    // Extract user/password from JDBC connection properties if its not supplied
    // in the connection URL
    if (!connParams.getSessionVars().containsKey(AUTH_USER)) {
      if (info.containsKey(AUTH_USER)) {
        connParams.getSessionVars().put(AUTH_USER, info.getProperty(AUTH_USER));
      }
      if (info.containsKey(AUTH_PASSWD)) {
        connParams.getSessionVars().put(AUTH_PASSWD, info.getProperty(AUTH_PASSWD));
      }
    }

    if (!connParams.getSessionVars().containsKey(AUTH_PASSWD)) {
      if (info.containsKey(AUTH_USER)) {
        connParams.getSessionVars().put(AUTH_USER, info.getProperty(AUTH_USER));
      }
      if (info.containsKey(AUTH_PASSWD)) {
        connParams.getSessionVars().put(AUTH_PASSWD, info.getProperty(AUTH_PASSWD));
      }
    }

    if (info.containsKey(AUTH_TYPE)) {
      connParams.getSessionVars().put(AUTH_TYPE, info.getProperty(AUTH_TYPE));
    }

    String authorityStr = connParams.getSuppliedURLAuthority();
    // If we're using ZooKeeper, the final host, port will be read from ZooKeeper
    // (in a different method call). Therefore, we put back the original authority string
    // (which basically is the ZooKeeper ensemble) back in the uri
    if (ZooKeeperHiveClientHelper.isZkDynamicDiscoveryMode(connParams.getSessionVars())) {
      uri = uri.replace(dummyAuthorityString, authorityStr);
      // Set ZooKeeper ensemble in connParams for later use
      connParams.setZooKeeperEnsemble(authorityStr);
    } else {
      URI jdbcBaseURI = URI.create(URI_HIVE_PREFIX + "//" + authorityStr);
      // Check to prevent unintentional use of embedded mode. A missing "/"
      // to separate the 'path' portion of URI can result in this.
      // The missing "/" common typo while using secure mode, eg of such url -
      // jdbc:hive2://localhost:10000;principal=hive/HiveServer2Host@YOUR-REALM.COM
      if (jdbcBaseURI.getAuthority() != null) {
        String host = jdbcBaseURI.getHost();
        int port = jdbcBaseURI.getPort();
        if (host == null) {
          throw new JdbcUriParseException(
              "Bad URL format. Hostname not found "
                  + " in authority part of the url: "
                  + jdbcBaseURI.getAuthority()
                  + ". Are you missing a '/' after the hostname ?");
        }
        // Set the port to default value; we do support jdbc url like:
        // jdbc:hive2://localhost/db
        if (port <= 0) {
          port = Integer.parseInt(Utils.DEFAULT_PORT);
        }
        connParams.setHost(host);
        connParams.setPort(port);
      }
      // We check for invalid host, port while configuring connParams with configureConnParams()
      authorityStr = connParams.getHost() + ":" + connParams.getPort();
      LOG.debug("Resolved authority: " + authorityStr);
      uri = uri.replace(dummyAuthorityString, authorityStr);
    }

    connParams.setJdbcUriString(uri);
    return connParams;
  }

  // Configure using ZooKeeper
  static void configureConnParamsFromZooKeeper(JdbcConnectionParams connParams)
      throws ZooKeeperHiveClientException, JdbcUriParseException {
    ZooKeeperHiveClientHelper.configureConnParams(connParams);
    String authorityStr = connParams.getHost() + ":" + connParams.getPort();
    LOG.debug("Resolved authority: " + authorityStr);
    String jdbcUriString = connParams.getJdbcUriString();
    // Replace ZooKeeper ensemble from the authority component of the JDBC Uri provided by the
    // client, by the host:port of the resolved server instance we will connect to
    connParams.setJdbcUriString(
        jdbcUriString.replace(getAuthorityFromJdbcURL(jdbcUriString), authorityStr));
  }

  private static void handleAllDeprecations(JdbcConnectionParams connParams) {
    // Handle all deprecations here:
    String newUsage;
    String usageUrlBase = "jdbc:hive2://<host>:<port>/dbName;";
    // Handle deprecation of AUTH_QOP_DEPRECATED
    newUsage = usageUrlBase + AUTH_QOP + "=<qop_value>";
    handleParamDeprecation(
        connParams.getSessionVars(),
        connParams.getSessionVars(),
        AUTH_QOP_DEPRECATED,
        AUTH_QOP,
        newUsage);

    // Handle deprecation of TRANSPORT_MODE_DEPRECATED
    newUsage = usageUrlBase + TRANSPORT_MODE + "=<transport_mode_value>";
    handleParamDeprecation(
        connParams.getHiveConfs(),
        connParams.getSessionVars(),
        TRANSPORT_MODE_DEPRECATED,
        TRANSPORT_MODE,
        newUsage);

    // Handle deprecation of HTTP_PATH_DEPRECATED
    newUsage = usageUrlBase + HTTP_PATH + "=<http_path_value>";
    handleParamDeprecation(
        connParams.getHiveConfs(),
        connParams.getSessionVars(),
        HTTP_PATH_DEPRECATED,
        HTTP_PATH,
        newUsage);
  }

  /**
   * Remove the deprecatedName param from the fromMap and put the key value in the toMap. Also log a
   * deprecation message for the client.
   */
  private static void handleParamDeprecation(
      Map<String, String> fromMap,
      Map<String, String> toMap,
      String deprecatedName,
      String newName,
      String newUsage) {
    if (fromMap.containsKey(deprecatedName)) {
      LOG.warn("***** JDBC param deprecation *****");
      LOG.warn("The use of " + deprecatedName + " is deprecated.");
      LOG.warn("Please use " + newName + " like so: " + newUsage);
      String paramValue = fromMap.remove(deprecatedName);
      toMap.put(newName, paramValue);
    }
  }

  /**
   * Get the authority string from the supplied uri, which could potentially contain multiple
   * host:port pairs.
   */
  private static String getAuthorityFromJdbcURL(String uri) throws JdbcUriParseException {
    String authorities;
    /*
     * For a jdbc uri like:
     * jdbc:hive2://<host1>:<port1>,<host2>:<port2>/dbName;sess_var_list?conf_list#var_list Extract
     * the uri host:port list starting after "jdbc:hive2://", till the 1st "/" or "?" or "#"
     * whichever comes first & in the given order Examples:
     * jdbc:hive2://host1:port1,host2:port2,host3:port3/db;k1=v1?k2=v2#k3=v3
     * jdbc:hive2://host1:port1,host2:port2,host3:port3/;k1=v1?k2=v2#k3=v3
     * jdbc:hive2://host1:port1,host2:port2,host3:port3?k2=v2#k3=v3
     * jdbc:hive2://host1:port1,host2:port2,host3:port3#k3=v3
     */
    String matchedUrlPrefix = getMatchedUrlPrefix(uri);
    int fromIndex = matchedUrlPrefix.length();
    int toIndex = -1;
    ArrayList<String> toIndexChars = new ArrayList<>(Arrays.asList("/", "?", "#"));
    for (String toIndexChar : toIndexChars) {
      toIndex = uri.indexOf(toIndexChar, fromIndex);
      if (toIndex > 0) {
        break;
      }
    }
    if (toIndex < 0) {
      authorities = uri.substring(fromIndex);
    } else {
      authorities = uri.substring(fromIndex, toIndex);
    }
    return authorities;
  }

  /**
   * Read the next server coordinates (host:port combo) from ZooKeeper. Ignore the znodes already
   * explored. Also update the host, port, jdbcUriString and other configs published by the server.
   *
   * @param connParams
   * @return true if new server info is retrieved successfully
   */
  static boolean updateConnParamsFromZooKeeper(JdbcConnectionParams connParams) {
    // Add current host to the rejected list
    connParams.getRejectedHostZnodePaths().add(connParams.getCurrentHostZnodePath());
    String oldServerHost = connParams.getHost();
    int oldServerPort = connParams.getPort();
    // Update connection params (including host, port) from ZooKeeper
    try {
      ZooKeeperHiveClientHelper.configureConnParams(connParams);
      connParams.setJdbcUriString(
          connParams
              .getJdbcUriString()
              .replace(
                  oldServerHost + ":" + oldServerPort,
                  connParams.getHost() + ":" + connParams.getPort()));
      LOG.info("Selected HiveServer2 instance with uri: " + connParams.getJdbcUriString());
    } catch (ZooKeeperHiveClientException e) {
      LOG.error(e.getMessage());
      return false;
    }

    return true;
  }

  /**
   * Takes a version string delimited by '.' and '-' characters and returns a partial version.
   *
   * @param fullVersion version string.
   * @param position position of version string to get starting at 0. eg, for a X.x.xxx string, 0
   *     will return the major version, 1 will return minor version.
   * @return version part, or -1 if version string was malformed.
   */
  static int getVersionPart(String fullVersion, int position) {
    int version = -1;
    try {
      String[] tokens = fullVersion.split("[.-]"); // $NON-NLS-1$

      if (tokens.length > 1 && tokens[position] != null) {
        version = Integer.parseInt(tokens[position]);
      }
    } catch (Exception ignore) {
    }
    return version;
  }

  public static String parsePropertyFromUrl(final String url, final String key) {
    String[] tokens = url.split(";");
    for (String token : tokens) {
      if (token.trim().startsWith(key.trim() + "=")) {
        return token.trim().substring((key.trim() + "=").length());
      }
    }
    return null;
  }

  /**
   * Method to get canonical-ized hostname, given a hostname (possibly a CNAME). This should allow
   * for service-principals to use simplified CNAMEs.
   *
   * @param hostName The hostname to be canonical-ized.
   * @return Given a CNAME, the canonical-ized hostname is returned. If not found, the original
   *     hostname is returned.
   */
  public static String getCanonicalHostName(String hostName) {
    try {
      InetAddress addr = InetAddress.getByName(hostName);
      String canonicalHostname = addr.getCanonicalHostName();
      if (canonicalHostname.equals(addr.getHostAddress())) {
        return hostName;
      } else {
        return canonicalHostname;
      }
    } catch (UnknownHostException exception) {
      LOG.warn("Could not retrieve canonical hostname for " + hostName, exception);
      return hostName;
    }
  }

  public static boolean isKyuubiOperationHint(String hint) {
    return KYUUBI_OPERATION_HINT_PATTERN.matcher(hint).matches();
  }

  public static final String KYUUBI_CLIENT_VERSION_KEY = "kyuubi.client.version";
  private static String KYUUBI_CLIENT_VERSION;

  public static synchronized String getVersion() {
    if (KYUUBI_CLIENT_VERSION == null) {
      try {
        Properties prop = new Properties();
        prop.load(
            Utils.class
                .getClassLoader()
                .getResourceAsStream("org/apache/kyuubi/version.properties"));
        KYUUBI_CLIENT_VERSION = prop.getProperty(KYUUBI_CLIENT_VERSION_KEY, "unknown");
      } catch (Exception e) {
        LOG.error("Error getting kyuubi client version", e);
        KYUUBI_CLIENT_VERSION = "unknown";
      }
    }
    return KYUUBI_CLIENT_VERSION;
  }

  /**
   * Method to get the password from the credential provider
   *
   * @param configuration Hadoop configuration
   * @param providerPath provider path
   * @param key alias name
   * @return password
   */
  private static String getPasswordFromCredentialProvider(
      Object configuration, String providerPath, String key) {
    try {
      if (providerPath != null) {
        DynMethods.builder("set")
            .impl(Class.forName(HADOOP_CONFIGURATION_CLASS), String.class, String.class)
            .buildChecked()
            .invoke(configuration, HADOOP_SECURITY_CREDENTIAL_PATH, providerPath);

        char[] password =
            DynMethods.builder("getPassword")
                .impl(Class.forName(HADOOP_CONFIGURATION_CLASS), String.class)
                .buildChecked()
                .invoke(configuration, key);
        if (password != null) {
          return new String(password);
        }
      }
    } catch (ClassNotFoundException exception) {
      throw new RuntimeException(exception);
    } catch (NoSuchMethodException exception) {
      LOG.warn("Could not retrieve password for " + key, exception);
      throw new RuntimeException(exception);
    }
    return null;
  }

  /**
   * Method to get the password from the configuration map if available. Otherwise, get it from the
   * Hadoop CredentialProvider if Hadoop classes are available
   *
   * @param confMap configuration map
   * @param key param
   * @return password
   */
  public static String getPassword(Map<String, String> confMap, String key) {
    String password = confMap.get(key);
    boolean hadoopCredentialProviderAvailable = false;
    Object hadoopConfiguration = null;
    if (password == null) {
      try {
        hadoopConfiguration =
            DynConstructors.builder().impl(HADOOP_CONFIGURATION_CLASS).build().newInstance();
        hadoopCredentialProviderAvailable = true;
      } catch (Exception exception) {
        LOG.warn("Hadoop credential provider is unavailable", exception);
        throw new RuntimeException(exception);
      }
    }
    if (password == null && hadoopCredentialProviderAvailable) {
      password =
          getPasswordFromCredentialProvider(
              hadoopConfiguration, confMap.get(JdbcConnectionParams.SSL_STORE_PASSWORD_PATH), key);
    }
    return password;
  }
}
