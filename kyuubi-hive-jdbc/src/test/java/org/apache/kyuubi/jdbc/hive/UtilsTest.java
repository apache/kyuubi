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

package org.apache.kyuubi.jdbc.hive;

import static org.junit.Assert.assertEquals;

import java.util.Properties;
import org.junit.Test;

public class UtilsTest {

  @Test
  public void testExtractURLComponentsWithAuthoritiesIsEmpty() throws JdbcUriParseException {
    String uri1 = "jdbc:hive2:///db;k1=v1?k2=v2#k3=v3";
    String uri2 = "jdbc:hive2:///";
    Utils.JdbcConnectionParams jdbcConnectionParams1 =
        Utils.extractURLComponents(uri1, new Properties());
    assertEquals("localhost", jdbcConnectionParams1.getHost());
    assertEquals(10009, jdbcConnectionParams1.getPort());

    Utils.JdbcConnectionParams jdbcConnectionParams2 =
        Utils.extractURLComponents(uri1, new Properties());
    assertEquals("localhost", jdbcConnectionParams2.getHost());
    assertEquals(10009, jdbcConnectionParams2.getPort());
  }

  @Test
  public void testExtractURLComponentsWithAuthoritiesIsNotEmpty() throws JdbcUriParseException {
    String uri = "jdbc:hive2://hostname:10018/db;k1=v1?k2=v2#k3=v3";
    Utils.JdbcConnectionParams jdbcConnectionParams =
        Utils.extractURLComponents(uri, new Properties());
    assertEquals("hostname", jdbcConnectionParams.getHost());
    assertEquals(10018, jdbcConnectionParams.getPort());
  }
}
