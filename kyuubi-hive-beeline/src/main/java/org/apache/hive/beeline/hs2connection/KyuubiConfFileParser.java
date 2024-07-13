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

import java.io.IOException;
import java.io.Reader;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import org.apache.kyuubi.util.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Looks for a kyuubi-defaults.conf from KYUUBI_CONF_DIR. If found, this class parses
 * the kyuubi-defaults.conf to return a set of connection properties which can be used
 * to construct the connection url for Beeline connection
 */
public class KyuubiConfFileParser implements HS2ConnectionFileParser {
  private static final Logger LOG = LoggerFactory.getLogger(KyuubiConfFileParser.class);
  private static final String TRUSTSTORE_PASS_PROP = "javax.net.ssl.trustStorePassword";
  private static final String TRUSTSTORE_PROP = "javax.net.ssl.trustStore";

  private final boolean loaded;
  private final Properties kyuubiDefaults;

  public KyuubiConfFileParser() {
    kyuubiDefaults = new Properties();
    Path filePath = Paths.get(System.getenv("KYUUBI_CONF_DIR"), "kyuubi-defaults.conf");
    if (Files.isRegularFile(filePath)) {
      LOG.info("Using {} to construct the connection URL", filePath.toAbsolutePath());
      try (Reader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
        kyuubiDefaults.load(reader);
      } catch (IOException rethrow) {
        throw new RuntimeException(rethrow);
      }
      loaded = true;
    } else {
      LOG.debug("kyuubi-defaults.conf not found for constructing the connection URL");
      loaded = false;
    }
  }

  // only for testing
  KyuubiConfFileParser(Properties mockedProperties) {
    kyuubiDefaults = mockedProperties;
    loaded = true;
  }

  @Override
  public Properties getConnectionProperties() throws KyuubiConfFileParseException {
    Properties props = new Properties();
    if (!configExists() || thriftMode() == THRIFT_MODE.DISABLED) {
      return props;
    }
    props.setProperty(HS2ConnectionFileParser.URL_PREFIX_PROPERTY_KEY, "jdbc:kyuubi://");
    addHosts(props);
    addSSL(props);
    addKerberos(props);
    addHttp(props);
    return props;
  }

  // TODO: Kyuubi has different logic to handle SSL and QOP properties with HiveServer2.
  //       We need to revise this part later
  private void addSSL(Properties props) {
    // if (!conf.getBoolean("hive.server2.use.SSL", false)) {
    //   return;
    // } else {
    //   props.setProperty("ssl", "true");
    // }
    // String truststore = System.getenv(TRUSTSTORE_PROP);
    // if (truststore != null && truststore.isEmpty()) {
    //   props.setProperty("sslTruststore", truststore);
    // }
    // String trustStorePassword = System.getenv(TRUSTSTORE_PASS_PROP);
    // if (trustStorePassword != null && !trustStorePassword.isEmpty()) {
    //   props.setProperty("trustStorePassword", trustStorePassword);
    // }
    String saslQop = kyuubiDefaults.getProperty("kyuubi.authentication.sasl.qop", "auth");
    if (!"auth".equalsIgnoreCase(saslQop)) {
      props.setProperty("sasl.qop", saslQop);
    }
  }

  private void addKerberos(Properties props) {
    if (kerberosEnabled()) {
      props.setProperty("principal", kyuubiDefaults.getProperty("kyuubi.kinit.principal"));
    }
  }

  private void addHttp(Properties props) {
    THRIFT_MODE thriftMode = thriftMode();
    assert thriftMode == THRIFT_MODE.BINARY || thriftMode == THRIFT_MODE.HTTP;

    if (thriftMode == THRIFT_MODE.HTTP) {
      props.setProperty("transportMode", "http");
      String path = kyuubiDefaults.getProperty("kyuubi.frontend.thrift.http.path", "cliservice");
      props.setProperty("httpPath", path);
    }
  }

  private void addHosts(Properties props) throws KyuubiConfFileParseException {
    THRIFT_MODE thriftMode = thriftMode();
    assert thriftMode == THRIFT_MODE.BINARY || thriftMode == THRIFT_MODE.HTTP;

    String host = kyuubiDefaults.getProperty("kyuubi.frontend.advertised.host");
    if (host == null) {
      if (thriftMode == THRIFT_MODE.BINARY) {
        host = kyuubiDefaults.getProperty("kyuubi.frontend.thrift.binary.bind.host");
      } else { // THRIFT_MODE.HTTP
        host = kyuubiDefaults.getProperty("kyuubi.frontend.thrift.http.bind.host");
      }
    }
    if (host == null) {
      host = kyuubiDefaults.getProperty("kyuubi.frontend.bind.host");
    }

    if (host == null) {
      try {
        String useHostname =
            kyuubiDefaults.getProperty("kyuubi.frontend.connection.url.use.hostname", "true");
        if (Boolean.parseBoolean(useHostname)) {
          host = JavaUtils.findLocalInetAddress().getCanonicalHostName();
        } else {
          host = JavaUtils.findLocalInetAddress().getHostAddress();
        }
      } catch (UnknownHostException | SocketException rethrow) {
        throw new KyuubiConfFileParseException(rethrow.getMessage(), rethrow);
      }
    }

    int portNum = getPortNum(thriftMode);
    props.setProperty("hosts", host + ":" + portNum);
  }

  private int getPortNum(THRIFT_MODE thriftMode) {
    assert thriftMode == THRIFT_MODE.BINARY || thriftMode == THRIFT_MODE.HTTP;

    String port;
    if (thriftMode == THRIFT_MODE.BINARY) {
      port = kyuubiDefaults.getProperty("kyuubi.frontend.thrift.binary.bind.port");
      if (port == null) {
        port = kyuubiDefaults.getProperty("kyuubi.frontend.bind.port", "10009");
      }
    } else { // THRIFT_MODE.HTTP
      port = kyuubiDefaults.getProperty("kyuubi.frontend.thrift.http.bind.port", "10010");
    }
    return Integer.parseInt(port);
  }

  @Override
  public boolean configExists() {
    return loaded;
  }

  private enum THRIFT_MODE {
    BINARY,
    HTTP,
    DISABLED
  }

  // Kyuubi supports enabling THRIFT_BINARY and THRIFT_HTTP at the same time, in such a case,
  // return THRIFT_MODE.BINARY
  private THRIFT_MODE thriftMode() {
    String[] protocols =
        kyuubiDefaults.getProperty("kyuubi.frontend.protocols", "THRIFT_BINARY,REST").split(",");
    for (String protocol : protocols) {
      if (protocol.equalsIgnoreCase("THRIFT_BINARY")) {
        return THRIFT_MODE.BINARY;
      } else if (protocol.equalsIgnoreCase("THRIFT_HTTP")) {
        return THRIFT_MODE.HTTP;
      }
    }
    return THRIFT_MODE.DISABLED;
  }

  private boolean kerberosEnabled() {
    for (String auth : kyuubiDefaults.getProperty("kyuubi.authentication", "NONE").split(",")) {
      if (auth.equalsIgnoreCase("KERBEROS")) {
        return true;
      }
    }
    return false;
  }
}
