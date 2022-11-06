/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.beeline;

import org.apache.hive.beeline.hs2connection.DefaultConnectionUrlParser;
import org.junit.Assert;
import org.junit.Test;

public class DefaultConnectionUrlParserTest {

  @Test
  public void testDefaultUrl_thrift_binary() {
    System.setProperty("kyuubi.beeline.thrift.binary.bind.host", "kyuubi.test.com");

    String url = new DefaultConnectionUrlParser().getConnectionUrl();
    String expected = "jdbc:hive2://kyuubi.test.com:10009/default";
    Assert.assertEquals(expected, url);

    System.clearProperty("kyuubi.beeline.thrift.binary.bind.host");
  }

  @Test
  public void testDefaultUrl_thrift_http() {
    System.setProperty("kyuubi.beeline.thrift.http.bind.host", "kyuubi.test.com");
    System.setProperty("kyuubi.beeline.thrift.http.bind.port", "10010");
    System.setProperty("kyuubi.beeline.thrift.transport.mode", "THRIFT_HTTP");

    String url = new DefaultConnectionUrlParser().getConnectionUrl();
    String expected =
        "jdbc:hive2://kyuubi.test.com:10010/default;transportMode=http;httpPath=cliservice";
    Assert.assertEquals(expected, url);

    System.clearProperty("kyuubi.beeline.thrift.http.bind.host");
    System.clearProperty("kyuubi.beeline.thrift.http.bind.port");
    System.clearProperty("kyuubi.beeline.thrift.transport.mode");
  }

  @Test
  public void testDefaultUrl_zookeeper() {
    System.setProperty(
        "kyuubi.beeline.ha.addresses", "zk-node-1:2181,zk-node-2:2181,zk-node-3:2181");
    System.setProperty("kyuubi.beeline.ha.namespace", "kyuubi_test");

    String url = new DefaultConnectionUrlParser().getConnectionUrl();
    String expected =
        "jdbc:hive2://zk-node-1:2181,zk-node-2:2181,zk-node-3:2181/default;"
            + "serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=kyuubi_test";
    Assert.assertEquals(expected, url);

    System.clearProperty("kyuubi.beeline.ha.addresses");
    System.clearProperty("kyuubi.beeline.ha.namespace");
  }

  @Test
  public void testDefaultUrl_kerberos() {
    System.setProperty("kyuubi.beeline.thrift.binary.bind.host", "kyuubi.test.com");
    System.setProperty("kyuubi.beeline.authentication", "KERBEROS, LDAP");
    System.setProperty(
        "kyuubi.beeline.kerberos.principal", "kyuubi/kyuubi.test.com@DEFAULT.TEST.COM");

    String url = new DefaultConnectionUrlParser().getConnectionUrl();
    String expected =
        "jdbc:hive2://kyuubi.test.com:10009/default;"
            + "principal=kyuubi/kyuubi.test.com@DEFAULT.TEST.COM";
    Assert.assertEquals(expected, url);

    System.clearProperty("kyuubi.beeline.thrift.binary.bind.host");
    System.clearProperty("kyuubi.beeline.authentication");
    System.clearProperty("kyuubi.beeline.kerberos.principal");
  }

  @Test
  public void testDefaultUrl_var_and_conf() {
    System.setProperty("kyuubi.beeline.thrift.binary.bind.host", "kyuubi.test.com");
    System.setProperty("kyuubi.beeline.sessionConf.hive.server2.proxy.user", "b_kyuubi");
    System.setProperty("kyuubi.beeline.kyuubiConf.kyuubi.engine.share.level", "CONNECTION");
    System.setProperty("kyuubi.beeline.kyuubiVar.spark.yarn.queue", "infra-test");
    System.setProperty("kyuubi.beeline.kyuubiVar.spark.ui.enabled", "false");

    String url = new DefaultConnectionUrlParser().getConnectionUrl();
    String expected =
        "jdbc:hive2://kyuubi.test.com:10009/default;hive.server2.proxy.user=b_kyuubi"
            + "?kyuubi.engine.share.level=CONNECTION#spark.ui.enabled=false;spark.yarn.queue=infra-test";
    Assert.assertEquals(expected, url);

    System.clearProperty("kyuubi.beeline.thrift.binary.bind.host");
    System.clearProperty("kyuubi.beeline.sessionConf.hive.server2.proxy.user");
    System.clearProperty("kyuubi.beeline.kyuubiConf.kyuubi.engine.share.level");
    System.clearProperty("kyuubi.beeline.kyuubiVar.spark.yarn.queue");
    System.clearProperty("kyuubi.beeline.kyuubiVar.spark.ui.enabled");
  }

  @Test
  public void testDefaultUrl_ssl() {
    System.setProperty("kyuubi.beeline.thrift.binary.bind.host", "kyuubi.test.com");
    System.setProperty("kyuubi.beeline.use.SSL", "true");
    System.setProperty("kyuubi.beeline.ssl.truststore", "/user/kyuubi/gateway.jks");
    System.setProperty("kyuubi.beeline.ssl.truststore.password", "testPassword");

    String url = new DefaultConnectionUrlParser().getConnectionUrl();
    String expected =
        "jdbc:hive2://kyuubi.test.com:10009/default;ssl=true;"
            + "sslTrustStore=/user/kyuubi/gateway.jks;trustStorePassword=testPassword";
    Assert.assertEquals(expected, url);

    System.clearProperty("kyuubi.beeline.thrift.binary.bind.host");
    System.clearProperty("kyuubi.beeline.use.SSL");
    System.clearProperty("kyuubi.beeline.ssl.truststore");
    System.clearProperty("kyuubi.beeline.ssl.truststore.password");
  }
}
