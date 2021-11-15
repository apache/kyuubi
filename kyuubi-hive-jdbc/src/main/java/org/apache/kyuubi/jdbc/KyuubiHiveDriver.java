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

package org.apache.kyuubi.jdbc;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.sql.*;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.logging.Logger;

import org.apache.kyuubi.jdbc.hive.Utils;
import org.apache.kyuubi.jdbc.hive.KyuubiConnection;

/**
 * Kyuubi JDBC driver to connect to Kyuubi server via HiveServer2 thrift protocol.
 */
public class KyuubiHiveDriver implements Driver {
  static {
    try {
      DriverManager.registerDriver(new KyuubiHiveDriver());
    } catch (SQLException e) {
      throw new RuntimeException("Failed to register driver", e);
    }
  }

  public static final String DBNAME_PROPERTY_KEY = "DBNAME";

  public static final String HOST_PROPERTY_KEY = "HOST";

  public static final String PORT_PROPERTY_KEY = "PORT";

  public static final String DEFAULT_PORT = "10009";

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return url.startsWith(Utils.URL_PREFIX);
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return acceptsURL(url) ? new KyuubiConnection(url, info) : null;
  }

  @Override
  public int getMajorVersion() {
    return getMajorDriverVersion();
  }

  @Override
  public int getMinorVersion() {
    return getMinorDriverVersion();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    if (info == null) {
      info = new Properties();
    }

    if ((url != null) && url.startsWith(Utils.URL_PREFIX)) {
      info = parseURLForPropertyInfo(url, info);
    }

    DriverPropertyInfo hostProp = new DriverPropertyInfo(HOST_PROPERTY_KEY,
      info.getProperty(HOST_PROPERTY_KEY, ""));
    hostProp.required = false;
    hostProp.description = "Hostname of Kyuubi Server";

    DriverPropertyInfo portProp = new DriverPropertyInfo(PORT_PROPERTY_KEY,
      info.getProperty(PORT_PROPERTY_KEY, ""));
    portProp.required = false;
    portProp.description = "Port number of Kyuubi Server";

    DriverPropertyInfo dbProp = new DriverPropertyInfo(DBNAME_PROPERTY_KEY,
      info.getProperty(DBNAME_PROPERTY_KEY, "default"));
    dbProp.required = false;
    dbProp.description = "Database name";

    DriverPropertyInfo[] dpi = new DriverPropertyInfo[3];

    dpi[0] = hostProp;
    dpi[1] = portProp;
    dpi[2] = dbProp;

    return dpi;
  }

  /**
   * Returns whether the driver is JDBC compliant.
   */
  @Override
  public boolean jdbcCompliant() {
    return false;
  }


  /**
   * Takes a url in the form of jdbc:hive://[hostname]:[port]/[db_name] and
   * parses it. Everything after jdbc:hive// is optional.
   * <p>
   * The output from Utils.parseUrl() is massaged for the needs of getPropertyInfo
   */
  private Properties parseURLForPropertyInfo(String url, Properties defaults) throws SQLException {
    Properties urlProps = (defaults != null) ? new Properties(defaults) : new Properties();

    if (url == null || !url.startsWith(Utils.URL_PREFIX)) {
      throw new SQLException("Invalid connection url: " + url);
    }

    Utils.JdbcConnectionParams params;
    try {
      Method parseURLMethod = Utils.class.getDeclaredMethod("parseURL", String.class, Properties.class);
      parseURLMethod.setAccessible(true);
      params = (Utils.JdbcConnectionParams) parseURLMethod.invoke(null, url, defaults);
    } catch (Exception e) {
      throw new SQLException(e);
    }
    String host = params.getHost();
    if (host == null) {
      host = "";
    }
    String port = Integer.toString(params.getPort());
    if (host.equals("")) {
      port = "";
    } else if (port.equals("0") || port.equals("-1")) {
      port = DEFAULT_PORT;
    }
    String db = params.getDbName();
    urlProps.put(HOST_PROPERTY_KEY, host);
    urlProps.put(PORT_PROPERTY_KEY, port);
    urlProps.put(DBNAME_PROPERTY_KEY, db);

    return urlProps;
  }

  /**
   * Lazy-load manifest attributes as needed.
   */
  private static Attributes manifestAttributes = null;

  /**
   * Loads the manifest attributes from the jar.
   */
  private static synchronized void loadManifestAttributes() throws IOException {
    if (manifestAttributes != null) {
      return;
    }
    Class<?> clazz = KyuubiHiveDriver.class;
    URL classContainer = clazz.getProtectionDomain().getCodeSource().getLocation();
    URL manifestUrl = new URL("jar:" + classContainer + "!/META-INF/MANIFEST.MF");
    Manifest manifest = new Manifest(manifestUrl.openStream());
    manifestAttributes = manifest.getMainAttributes();
  }

  /**
   * Package scoped to allow manifest fetching from other KyuubiHiveDriver classes
   * Helper to initialize attributes and return one.
   */
  public static String fetchManifestAttribute(Attributes.Name attributeName) throws SQLException {
    try {
      loadManifestAttributes();
    } catch (IOException e) {
      throw new SQLException("Couldn't load manifest attributes.", e);
    }
    return manifestAttributes.getValue(attributeName);
  }

  /**
   * Package scoped access to the Driver's Major Version
   *
   * @return The Major version number of the driver. -1 if it cannot be determined from the
   * manifest.mf file.
   */
  public static int getMajorDriverVersion() {
    int version = -1;
    try {
      String fullVersion = fetchManifestAttribute(Attributes.Name.IMPLEMENTATION_VERSION);
      String[] tokens = fullVersion.split("\\.");

      if (tokens.length > 0 && tokens[0] != null) {
        version = Integer.parseInt(tokens[0]);
      }
    } catch (Exception e) {
      // Possible reasons to end up here:
      // - Unable to read version from manifest.mf
      // - Version string is not in the proper X.x.xxx format
    }
    return version;
  }

  /**
   * Package scoped access to the Driver's Minor Version
   *
   * @return The Minor version number of the driver. -1 if it cannot be determined from the
   * manifest.mf file.
   */
  public static int getMinorDriverVersion() {
    int version = -1;
    try {
      String fullVersion = fetchManifestAttribute(Attributes.Name.IMPLEMENTATION_VERSION);
      String[] tokens = fullVersion.split("\\.");

      if (tokens.length > 1 && tokens[1] != null) {
        version = Integer.parseInt(tokens[1]);
      }
    } catch (Exception e) {
      // Possible reasons to end up here:
      // - Unable to read version from manifest.mf
      // - Version string is not in the proper X.x.xxx format
    }
    return version;
  }
}
