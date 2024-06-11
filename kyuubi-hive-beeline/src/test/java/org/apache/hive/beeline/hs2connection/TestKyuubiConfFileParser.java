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

import static org.junit.Assert.assertEquals;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Properties;
import org.apache.kyuubi.util.JavaUtils;
import org.junit.Test;

public class TestKyuubiConfFileParser {

  @Test
  public void testParseKerberosEnabled()
      throws KyuubiConfFileParseException, SocketException, UnknownHostException {
    Properties kyuubiConf = new Properties();
    kyuubiConf.put("kyuubi.authentication", "KERBEROS");
    kyuubiConf.put("kyuubi.kinit.principal", "kyuubi/_HOST@EXAMPLE.COM");
    KyuubiConfFileParser kyuubiConfFileParser = new KyuubiConfFileParser(kyuubiConf);
    Properties result = kyuubiConfFileParser.getConnectionProperties();
    assertEquals(3, result.size());
    assertEquals(
        JavaUtils.findLocalInetAddress().getCanonicalHostName() + ":10009", result.get("hosts"));
    assertEquals("jdbc:kyuubi://", result.get(HS2ConnectionFileParser.URL_PREFIX_PROPERTY_KEY));
    assertEquals("kyuubi/_HOST@EXAMPLE.COM", result.get("principal"));
  }
}
