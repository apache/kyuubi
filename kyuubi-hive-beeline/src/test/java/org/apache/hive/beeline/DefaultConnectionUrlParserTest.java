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

import java.util.Properties;
import org.apache.hive.beeline.hs2connection.BeelineHS2ConnectionFileParseException;
import org.apache.hive.beeline.hs2connection.DefaultConnectionUrlParser;
import org.apache.hive.beeline.hs2connection.HS2ConnectionFileUtils;
import org.junit.Assert;
import org.junit.Test;

public class DefaultConnectionUrlParserTest {

  @Test
  public void testDefaultUrl_thrift_binary() throws BeelineHS2ConnectionFileParseException {
    System.setProperty("kyuubi.frontend.thrift.binary.bind.host", "kyuubi.test.com");
    System.setProperty("kyuubi.frontend.protocols", "THRIFT_BINARY");

    Properties properties = new DefaultConnectionUrlParser().getConnectionProperties();
    String url = HS2ConnectionFileUtils.getUrl(properties);
    String expected = "jdbc:hive2://kyuubi.test.com:10009/default";
    Assert.assertEquals(expected, url);

    System.clearProperty("kyuubi.frontend.thrift.binary.bind.host");
    System.clearProperty("kyuubi.frontend.protocols");
  }

  @Test
  public void testDefaultUrl_thrift_http() throws BeelineHS2ConnectionFileParseException {
    System.setProperty("kyuubi.frontend.thrift.http.bind.host", "kyuubi.test.com");
    System.setProperty("kyuubi.frontend.thrift.http.bind.port", "10010");
    System.setProperty("kyuubi.frontend.protocols", "THRIFT_HTTP");

    Properties properties = new DefaultConnectionUrlParser().getConnectionProperties();
    String url = HS2ConnectionFileUtils.getUrl(properties);
    String expected = "jdbc:hive2://kyuubi.test.com:10010/default;httpPath=cliservice;transportMode=http";
    Assert.assertEquals(expected, url);

    System.clearProperty("kyuubi.frontend.thrift.http.bind.host");
    System.clearProperty("kyuubi.frontend.thrift.http.bind.port");
    System.clearProperty("kyuubi.frontend.protocols");
  }

  @Test
  public void testDefaultUrl_zookeeper() throws BeelineHS2ConnectionFileParseException {
    System.setProperty("kyuubi.ha.addresses", "zk-node-1:2181,zk-node-2:2181,zk-node-3:2181");
    System.setProperty("kyuubi.ha.namespace", "kyuubi_test");

    Properties properties = new DefaultConnectionUrlParser().getConnectionProperties();
    String url = HS2ConnectionFileUtils.getUrl(properties);
    String expected = "jdbc:hive2://zk-node-1:2181,zk-node-2:2181,zk-node-3:2181/default;" +
            "serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=kyuubi_test";
    Assert.assertEquals(expected, url);

    System.clearProperty("kyuubi.ha.addresses");
    System.clearProperty("kyuubi.ha.namespace");
  }

  @Test
  public void testDefaultUrl_kerberos() throws BeelineHS2ConnectionFileParseException {
    System.setProperty("kyuubi.frontend.thrift.binary.bind.host", "kyuubi.test.com");
    System.setProperty("kyuubi.authentication", "KERBEROS, LDAP");
    System.setProperty("kyuubi.kinit.principal", "kyuubi/kyuubi.test.com@DEFAULT.TEST.COM");

    Properties properties = new DefaultConnectionUrlParser().getConnectionProperties();
    String url = HS2ConnectionFileUtils.getUrl(properties);
    String expected = "jdbc:hive2://kyuubi.test.com:10009/default;" +
            "principal=kyuubi/kyuubi.test.com@DEFAULT.TEST.COM";
    Assert.assertEquals(expected, url);

    System.clearProperty("kyuubi.frontend.thrift.binary.bind.host");
    System.clearProperty("kyuubi.authentication");
    System.clearProperty("kyuubi.kinit.principal");
  }

  // TODO: add ssl test

}
