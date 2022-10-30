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

package org.apache.hive.beeline.hs2connection;

import java.net.InetAddress;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.apache.kyuubi.Utils;
import org.apache.kyuubi.config.KyuubiConf;
import org.apache.kyuubi.ha.HighAvailabilityConf;

public class DefaultConnectionUrlParser {

  private KyuubiConf conf;

  public DefaultConnectionUrlParser() {
    this.conf = new KyuubiConf(true).loadFileDefaults();
  }

  public Properties getConnectionProperties() {
    Properties props = new Properties();
    props.setProperty("url_prefix", "jdbc:hive2://");
    this.addHosts(props);
    this.addSSL(props);
    this.addKerberos(props);
    this.addHttp(props);
    return props;
  }

  private void addHosts(Properties props) {
    if (StringUtils.isNotBlank(conf.get(HighAvailabilityConf.HA_ADDRESSES()))
        && StringUtils.isNotBlank(conf.get(HighAvailabilityConf.HA_NAMESPACE()))) {
      this.addZKServiceDiscoveryHosts(props);
    } else {
      this.addDefaultHS2Hosts(props);
    }
  }

  private void addZKServiceDiscoveryHosts(Properties props) {
    props.setProperty("serviceDiscoveryMode", "zooKeeper");
    props.setProperty("zooKeeperNamespace", conf.get(HighAvailabilityConf.HA_NAMESPACE()));
    props.setProperty("hosts", conf.get(HighAvailabilityConf.HA_ADDRESSES()));
  }

  private void addDefaultHS2Hosts(Properties props) {
    String host = conf.get(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_HOST()).getOrElse(() -> null);

    if (host == null) {
      InetAddress server = Utils.findLocalInetAddress();
      boolean useHostname = (boolean) conf.get(KyuubiConf.FRONTEND_CONNECTION_URL_USE_HOSTNAME());
      host = useHostname ? server.getCanonicalHostName() : server.getAddress().toString();
    }

    int portNum = (int) conf.get(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT());
    props.setProperty("hosts", host + ":" + portNum);
  }

  private void addHttp(Properties props) {
    String protocols = conf.get(KyuubiConf.FRONTEND_PROTOCOLS()).mkString();
    if (protocols.contains("THRIFT_HTTP")) {
      props.setProperty("transportMode", "http");
      props.setProperty("httpPath", conf.get(KyuubiConf.FRONTEND_THRIFT_HTTP_PATH()));
    }
  }

  private void addKerberos(Properties props) {
    String authMethods = conf.get(KyuubiConf.AUTHENTICATION_METHOD()).mkString();
    if (authMethods.contains("KERBEROS")) {
      props.setProperty("principal", conf.get(KyuubiConf.SERVER_PRINCIPAL()).get());
    }
  }

  private void addSSL(Properties props) {
    if ((boolean) conf.get(KyuubiConf.FRONTEND_THRIFT_BINARY_SSL_ENABLED())) {
      props.setProperty("ssl", "true");
      String truststore = System.getenv("javax.net.ssl.trustStore");
      if (truststore != null && truststore.isEmpty()) {
        props.setProperty("sslTruststore", truststore);
      }

      String trustStorePassword = System.getenv("javax.net.ssl.trustStorePassword");
      if (trustStorePassword != null && !trustStorePassword.isEmpty()) {
        props.setProperty("trustStorePassword", trustStorePassword);
      }

      String saslQop = conf.get(KyuubiConf.SASL_QOP());
      if (!"auth".equalsIgnoreCase(saslQop)) {
        props.setProperty("sasl.qop", saslQop);
      }
    }
  }
}
