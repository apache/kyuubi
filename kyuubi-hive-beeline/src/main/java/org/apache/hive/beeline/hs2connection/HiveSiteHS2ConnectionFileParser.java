/*
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
package org.apache.hive.beeline.hs2connection;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Looks for a hive-site.xml from the classpath. If found this class parses the hive-site.xml
 * to return a set of connection properties which can be used to construct the connection url
 * for Beeline connection
 *
 * This class is modified to get rid of dependency on HiveConf
 */
public class HiveSiteHS2ConnectionFileParser implements HS2ConnectionFileParser {
  private Configuration conf;
  private final URL hiveSiteURI;
  private static final String TRUSTSTORE_PASS_PROP = "javax.net.ssl.trustStorePassword";
  private static final String TRUSTSTORE_PROP = "javax.net.ssl.trustStore";
  private static final Logger LOG = LoggerFactory.getLogger(HiveSiteHS2ConnectionFileParser.class);

  // Copied from HiveConf
  private static URL findConfigFile(ClassLoader classLoader, String name, boolean doLog) {
    URL result = classLoader.getResource(name);
    if (result == null) {
      String confPath = System.getenv("HIVE_CONF_DIR");
      result = checkConfigFile(new File(confPath, name));
      if (result == null) {
        String homePath = System.getenv("HIVE_HOME");
        String nameInConf = "conf" + File.separator + name;
        result = checkConfigFile(new File(homePath, nameInConf));
        if (result == null) {
          URI jarUri = null;
          try {
            // Handle both file:// and jar:<url>!{entry} in the case of shaded hive libs
            URL sourceUrl =
                HiveSiteHS2ConnectionFileParser.class
                    .getProtectionDomain()
                    .getCodeSource()
                    .getLocation();
            jarUri =
                sourceUrl.getProtocol().equalsIgnoreCase("jar")
                    ? new URI(sourceUrl.getPath())
                    : sourceUrl.toURI();
          } catch (Throwable e) {
            LOG.info("Cannot get jar URI", e);
          }
          // From the jar file, the parent is /lib folder
          File parent = new File(jarUri).getParentFile();
          if (parent != null) {
            result = checkConfigFile(new File(parent.getParentFile(), nameInConf));
          }
        }
      }
    }
    if (doLog) {
      LOG.info("Found configuration file {}", result);
    }
    return result;
  }

  // Copied from HiveConf
  private static URL checkConfigFile(File f) {
    try {
      return (f.exists() && f.isFile()) ? f.toURI().toURL() : null;
    } catch (Throwable e) {
      LOG.info("Error looking for config {}", f, e);
      return null;
    }
  }

  public HiveSiteHS2ConnectionFileParser() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = this.getClass().getClassLoader();
    }
    // Look for hive-site.xml on the CLASSPATH and log its location if found.
    hiveSiteURI = findConfigFile(classLoader, "hive-site.xml", true);
    conf = new Configuration();
    if (hiveSiteURI == null) {
      LOG.debug("hive-site.xml not found for constructing the connection URL");
    } else {
      LOG.info("Using hive-site.xml at " + hiveSiteURI);
      conf.addResource(hiveSiteURI);
    }
  }

  @VisibleForTesting
  void setHiveConf(Configuration hiveConf) {
    this.conf = hiveConf;
  }

  @Override
  public Properties getConnectionProperties() throws BeelineHS2ConnectionFileParseException {
    Properties props = new Properties();
    if (!configExists()) {
      return props;
    }
    props.setProperty(HS2ConnectionFileParser.URL_PREFIX_PROPERTY_KEY, "jdbc:hive2://");
    addHosts(props);
    addSSL(props);
    addKerberos(props);
    addHttp(props);
    return props;
  }

  private void addSSL(Properties props) {
    if (!conf.getBoolean("hive.server2.use.SSL", false)) {
      return;
    } else {
      props.setProperty("ssl", "true");
    }
    String truststore = System.getenv(TRUSTSTORE_PROP);
    if (truststore != null && truststore.isEmpty()) {
      props.setProperty("sslTruststore", truststore);
    }
    String trustStorePassword = System.getenv(TRUSTSTORE_PASS_PROP);
    if (trustStorePassword != null && !trustStorePassword.isEmpty()) {
      props.setProperty("trustStorePassword", trustStorePassword);
    }
    String saslQop = conf.get("hive.server2.thrift.sasl.qop", "auth");
    if (!"auth".equalsIgnoreCase(saslQop)) {
      props.setProperty("sasl.qop", saslQop);
    }
  }

  private void addKerberos(Properties props) {
    if ("KERBEROS".equals(conf.get("hive.server2.authentication", "NONE"))) {
      props.setProperty(
          "principal", conf.get("hive.server2.authentication.kerberos.principal", ""));
    }
  }

  private void addHttp(Properties props) {
    if ("http".equalsIgnoreCase(conf.get("hive.server2.transport.mode", "binary"))) {
      props.setProperty("transportMode", "http");
    } else {
      return;
    }
    props.setProperty("httpPath", conf.get("hive.server2.thrift.http.path", "cliservice"));
  }

  private void addHosts(Properties props) throws BeelineHS2ConnectionFileParseException {
    // if zk HA is enabled get hosts property
    if (conf.getBoolean("hive.server2.support.dynamic.service.discovery", false)) {
      addZKServiceDiscoveryHosts(props);
    } else {
      addDefaultHS2Hosts(props);
    }
  }

  private void addZKServiceDiscoveryHosts(Properties props)
      throws BeelineHS2ConnectionFileParseException {
    props.setProperty("serviceDiscoveryMode", "zooKeeper");
    props.setProperty(
        "zooKeeperNamespace", conf.get("hive.server2.zookeeper.namespace", "hiveserver2"));
    props.setProperty("hosts", conf.get("hive.zookeeper.quorum", ""));
  }

  /**
   * Get the Inet address of the machine of the given host name.
   *
   * @param hostname The name of the host
   * @return The network address of the the host
   * @throws UnknownHostException
   */
  // Copied from ServerUtils
  private static InetAddress getHostAddress(String hostname) throws UnknownHostException {
    InetAddress serverIPAddress;
    if (hostname != null && !hostname.isEmpty()) {
      serverIPAddress = InetAddress.getByName(hostname);
    } else {
      serverIPAddress = InetAddress.getLocalHost();
    }
    return serverIPAddress;
  }

  private void addDefaultHS2Hosts(Properties props) throws BeelineHS2ConnectionFileParseException {
    String hiveHost = System.getenv("HIVE_SERVER2_THRIFT_BIND_HOST");
    if (hiveHost == null) {
      hiveHost = conf.get("hive.server2.thrift.bind.host", "");
    }

    InetAddress serverIPAddress;
    try {
      serverIPAddress = getHostAddress(hiveHost);
    } catch (UnknownHostException e) {
      throw new BeelineHS2ConnectionFileParseException(e.getMessage(), e);
    }
    int portNum =
        getPortNum("http".equalsIgnoreCase(conf.get("hive.server2.transport.mode", "binary")));
    props.setProperty("hosts", serverIPAddress.getHostName() + ":" + portNum);
  }

  private int getPortNum(boolean isHttp) {
    String portString;
    int portNum;
    if (isHttp) {
      portString = System.getenv("HIVE_SERVER2_THRIFT_HTTP_PORT");
      if (portString != null) {
        portNum = Integer.parseInt(portString);
      } else {
        portNum = conf.getInt("hive.server2.thrift.http.port", 10001);
      }
    } else {
      portString = System.getenv("HIVE_SERVER2_THRIFT_PORT");
      if (portString != null) {
        portNum = Integer.parseInt(portString);
      } else {
        portNum = conf.getInt("hive.server2.thrift.port", 10000);
      }
    }
    return portNum;
  }

  @Override
  public boolean configExists() {
    return (hiveSiteURI != null);
  }
}
