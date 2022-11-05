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
import org.apache.kyuubi.beeline.BeelineConf;
import org.apache.kyuubi.config.KyuubiConf;

public class DefaultConnectionUrlParser {

  public static String URL_PREFIX_PROPERTY_KEY = "url_prefix";
  public static String DB_NAME_PROPERTY_KEY = "dbName";
  public static String HOST_PROPERTY_KEY = "hosts";
  public static String SESSION_CONF_PROPERTY_KEY = "sessionconf";
  public static String KYUUBI_CONF_PROPERTY_KEY = "kyuubiconf";
  public static String KYUUBI_VAR_PROPERTY_KEY = "kyuubivar";

  private KyuubiConf conf;

  public DefaultConnectionUrlParser() {
    this.conf = new KyuubiConf(true).loadFileDefaults();
  }

  public Properties getConnectionProperties() {
    Properties props = new Properties();
    props.setProperty(URL_PREFIX_PROPERTY_KEY, "jdbc:hive2://");
    this.addHosts(props);
    this.addDb(props);
    this.addSSL(props);
    this.addKerberos(props);
    this.addHttp(props);
    this.addSessionConfs(props);
    this.addKyuubiConfs(props);
    this.addKyuubiVars(props);
    return props;
  }

  private void addHosts(Properties props) {
    if (StringUtils.isNotBlank(conf.get(BeelineConf.BEELINE_HA_ADDRESSES()))
        && StringUtils.isNotBlank(conf.get(BeelineConf.BEELINE_HA_NAMESPACE()))) {
      this.addZKServiceDiscoveryHosts(props);
    } else {
      this.addDefaultHS2Hosts(props);
    }
  }

  private void addZKServiceDiscoveryHosts(Properties props) {
    props.setProperty("serviceDiscoveryMode", "zooKeeper");
    props.setProperty("zooKeeperNamespace", conf.get(BeelineConf.BEELINE_HA_NAMESPACE()));
    props.setProperty("hosts", conf.get(BeelineConf.BEELINE_HA_ADDRESSES()));
  }

  private void addDefaultHS2Hosts(Properties props) {
    String host = null;
    int portNum = 0;
    String protocol = conf.get(BeelineConf.BEELINE_THRIFT_TRANSPORT_MODE());
    switch (protocol) {
      case "THRIFT_BINARY":
        host = conf.get(BeelineConf.BEELINE_THRIFT_BINARY_BIND_HOST()).getOrElse(() -> null);
        portNum = (int) conf.get(BeelineConf.BEELINE_THRIFT_BINARY_BIND_PORT());
        break;
      case "THRIFT_HTTP":
        host = conf.get(BeelineConf.BEELINE_THRIFT_HTTP_BIND_HOST()).getOrElse(() -> null);
        portNum = (int) conf.get(BeelineConf.BEELINE_THRIFT_HTTP_BIND_PORT());
        break;
      default:
        break;
    }

    if (host == null) {
      InetAddress server = Utils.findLocalInetAddress();
      host = server.getHostAddress();
    }

    props.setProperty(HOST_PROPERTY_KEY, host + ":" + portNum);
  }

  private void addDb(Properties props) {
    props.setProperty(DB_NAME_PROPERTY_KEY, conf.get(BeelineConf.BEELINE_DB_NAME()));
  }

  private void addHttp(Properties props) {
    String protocol = conf.get(BeelineConf.BEELINE_THRIFT_TRANSPORT_MODE());
    if (protocol.equalsIgnoreCase("THRIFT_HTTP")) {
      props.setProperty("transportMode", "http");
      props.setProperty("httpPath", conf.get(KyuubiConf.FRONTEND_THRIFT_HTTP_PATH()));
    }
  }

  private void addKerberos(Properties props) {
    String authMethods = conf.get(BeelineConf.BEELINE_AUTHENTICATION_METHOD()).mkString();
    if (authMethods.contains("KERBEROS")) {
      props.setProperty("principal", conf.get(BeelineConf.BEELINE_KERBEROS_PRINCIPAL()));
    }
  }

  private void addSSL(Properties props) {
    if ((boolean) conf.get(BeelineConf.BEELINE_USE_SSL())) {
      props.setProperty("ssl", "true");

      conf.get(BeelineConf.BEELINE_SSL_TRUSTSTORE())
          .filter(StringUtils::isNotBlank)
          .foreach(v -> props.setProperty("sslTrustStore", v));

      conf.get(BeelineConf.BEELINE_SSL_TRUSTSTORE_PASSWORD())
          .filter(StringUtils::isNotBlank)
          .foreach(v -> props.setProperty("trustStorePassword", v));

      String saslQop = conf.get(BeelineConf.BEELINE_SASL_QOP());
      if (!"auth".equalsIgnoreCase(saslQop)) {
        props.setProperty("sasl.qop", saslQop);
      }
    }
  }

  private void addSessionConfs(Properties props) {
    conf.get(BeelineConf.BEELINE_SESSION_CONFS())
        .foreach(v -> props.setProperty(SESSION_CONF_PROPERTY_KEY, v));
  }

  private void addKyuubiConfs(Properties props) {
    conf.get(BeelineConf.BEELINE_KYUUBI_CONFS())
        .foreach(v -> props.setProperty(KYUUBI_CONF_PROPERTY_KEY, v));
  }

  private void addKyuubiVars(Properties props) {
    conf.get(BeelineConf.BEELINE_KYUUBI_VARS())
        .foreach(v -> props.setProperty(KYUUBI_VAR_PROPERTY_KEY, v));
  }
}
