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

import com.google.common.base.Joiner;
import java.io.File;
import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.kyuubi.Utils;
import org.apache.kyuubi.beeline.BeelineConf;
import org.apache.kyuubi.config.KyuubiConf;
import org.apache.logging.log4j.util.Strings;

public class DefaultConnectionUrlParser {

  private KyuubiConf conf;

  private String host;
  private String dbName;
  private Map<String, String> sessionConfs = new LinkedHashMap<>();
  private Map<String, String> kyuubiConfs = new LinkedHashMap<>();
  private Map<String, String> kyuubiVars = new LinkedHashMap<>();

  public DefaultConnectionUrlParser() {
    this.conf = new KyuubiConf(true).loadFileDefaults();
    setHosts();
    setDb();
    setSSL();
    setKerberos();
    setHttp();
    setSessionConfs();
    setKyuubiConfs();
    setKyuubiVars();
  }

  /**
   * JDBC URLs have the following format:
   * jdbc:hive2://<host>:<port>/<dbName>;<sessionVars>?<kyuubiConfs>#<[spark|hive]Vars>
   */
  public String getConnectionUrl() {
    if (Strings.isBlank(host) || Strings.isBlank(dbName)) {
      return null;
    }

    StringBuilder urlSb = new StringBuilder();
    urlSb.append("jdbc:hive2://");
    urlSb.append(this.host.trim());
    urlSb.append(File.separator);
    urlSb.append(this.dbName.trim());

    if (this.sessionConfs.size() > 0) {
      urlSb.append(";");
      String sessionConfStr = Joiner.on(";").withKeyValueSeparator("=").join(this.sessionConfs);
      urlSb.append(sessionConfStr);
    }

    if (this.kyuubiConfs.size() > 0) {
      urlSb.append("?");
      String kyuubiConfStr = Joiner.on(";").withKeyValueSeparator("=").join(this.kyuubiConfs);
      urlSb.append(kyuubiConfStr);
    }

    if (this.kyuubiVars.size() > 0) {
      urlSb.append("#");
      String kyuubiVarStr = Joiner.on(";").withKeyValueSeparator("=").join(this.kyuubiVars);
      urlSb.append(kyuubiVarStr);
    }

    return urlSb.toString();
  }

  public void addSessionConfs(String key, String value) {
    this.sessionConfs.put(key, value);
  }

  private void setHosts() {
    if (StringUtils.isNotBlank(conf.get(BeelineConf.BEELINE_HA_ADDRESSES()))
        && StringUtils.isNotBlank(conf.get(BeelineConf.BEELINE_HA_NAMESPACE()))) {
      this.addZKServiceDiscoveryHosts();
    } else {
      this.addDefaultHS2Hosts();
    }
  }

  private void addZKServiceDiscoveryHosts() {
    this.host = conf.get(BeelineConf.BEELINE_HA_ADDRESSES());
    this.sessionConfs.put("serviceDiscoveryMode", "zooKeeper");
    this.sessionConfs.put("zooKeeperNamespace", conf.get(BeelineConf.BEELINE_HA_NAMESPACE()));
  }

  private void addDefaultHS2Hosts() {
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

    this.host = host + ":" + portNum;
  }

  private void setDb() {
    this.dbName = conf.get(BeelineConf.BEELINE_DB_NAME());
  }

  private void setHttp() {
    String protocol = conf.get(BeelineConf.BEELINE_THRIFT_TRANSPORT_MODE());
    if (protocol.equalsIgnoreCase("THRIFT_HTTP")) {
      this.sessionConfs.put("transportMode", "http");
      this.sessionConfs.put("httpPath", conf.get(KyuubiConf.FRONTEND_THRIFT_HTTP_PATH()));
    }
  }

  private void setKerberos() {
    String authMethods = conf.get(BeelineConf.BEELINE_AUTHENTICATION_METHOD()).mkString();
    if (authMethods.contains("KERBEROS")) {
      this.sessionConfs.put("principal", conf.get(BeelineConf.BEELINE_KERBEROS_PRINCIPAL()));
    }
  }

  private void setSSL() {
    if ((boolean) conf.get(BeelineConf.BEELINE_USE_SSL())) {
      this.sessionConfs.put("ssl", "true");

      conf.get(BeelineConf.BEELINE_SSL_TRUSTSTORE())
          .filter(StringUtils::isNotBlank)
          .foreach(v -> this.sessionConfs.put("sslTrustStore", v));

      conf.get(BeelineConf.BEELINE_SSL_TRUSTSTORE_PASSWORD())
          .filter(StringUtils::isNotBlank)
          .foreach(v -> this.sessionConfs.put("trustStorePassword", v));

      String saslQop = conf.get(BeelineConf.BEELINE_SASL_QOP());
      if (!"auth".equalsIgnoreCase(saslQop)) {
        this.sessionConfs.put("sasl.qop", saslQop);
      }
    }
  }

  private void setSessionConfs() {
    Map<String, String> sessionConfs = BeelineConf.getBeelineSessionConfs(conf);
    this.sessionConfs.putAll(sessionConfs);
  }

  private void setKyuubiConfs() {
    Map<String, String> sessionConfs = BeelineConf.getBeelineKyuubiConfs(conf);
    this.kyuubiConfs.putAll(sessionConfs);
  }

  private void setKyuubiVars() {
    Map<String, String> sessionConfs = BeelineConf.getBeelineKyuubiVars(conf);
    this.kyuubiVars.putAll(sessionConfs);
  }
}
