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

package org.apache.kyuubi.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kyuubi.util.IPStackUtils.HostPort;
import org.junit.jupiter.api.Test;

public class IPStackUtilsTest {

  // Test cases for concatHostPort method

  @Test
  public void testConcatHostPort() {
    assertEquals("192.168.1.1:8080", IPStackUtils.concatHostPort("192.168.1.1", 8080));
    assertEquals("[2001:db8::1]:8080", IPStackUtils.concatHostPort("2001:db8::1", 8080));
    assertEquals("[::1]:9090", IPStackUtils.concatHostPort("::1", 9090));
    assertEquals("example.com:443", IPStackUtils.concatHostPort("example.com", 443));
  }

  // Test cases for getHostAndPort method

  @Test
  public void testGetHostAndPortWithIPv4() {
    HostPort result = IPStackUtils.getHostAndPort("192.168.1.1:8080");
    assertEquals("192.168.1.1", result.getHostname());
    assertEquals(8080, result.getPort());
  }

  @Test
  public void testGetHostAndPortWithValidIPv6WithSquaredBrackets() {
    HostPort result = IPStackUtils.getHostAndPort("[2001:0db8::1]:8080");
    assertEquals("2001:0db8::1", result.getHostname());
    assertEquals(8080, result.getPort());
  }

  @Test
  public void testGetHostAndPortWithValidIPv6WithoutSquaredBrackets() {
    HostPort result = IPStackUtils.getHostAndPort("2001:0db8::1:8080");
    assertEquals("2001:0db8::1", result.getHostname());
    assertEquals(8080, result.getPort());
  }

  @Test
  public void testGetHostAndPortWithHostname() {
    HostPort result = IPStackUtils.getHostAndPort("example.com:80");
    assertEquals("example.com", result.getHostname());
    assertEquals(80, result.getPort());
  }

  @Test
  public void testGetHostPortWithInvalidAndPort() {
    // Test case: port number out of range
    IllegalArgumentException e1 =
        assertThrows(
            IllegalArgumentException.class, () -> IPStackUtils.getHostAndPort("192.168.1.1:70000"));
    assertEquals("Port number out of range (0-65535).", e1.getMessage());

    // Test case: input missing port
    IllegalArgumentException e2 =
        assertThrows(
            IllegalArgumentException.class, () -> IPStackUtils.getHostAndPort("192.168.1.1"));
    assertEquals("Input does not contain a port.", e2.getMessage());

    // Test case: missing host
    IllegalArgumentException e3 =
        assertThrows(IllegalArgumentException.class, () -> IPStackUtils.getHostAndPort(":8080"));
    assertEquals("Host address is null or empty.", e3.getMessage());
  }

  // Test cases for getPort method

  @Test
  public void testGetPort() {
    assertEquals(8080, IPStackUtils.getPort("8080"));
    assertEquals(65535, IPStackUtils.getPort("65535"));
    assertEquals(0, IPStackUtils.getPort("0"));
  }

  @Test
  public void testGetPortWithInvalidPort() {
    // Test case: port number too high
    IllegalArgumentException e1 =
        assertThrows(IllegalArgumentException.class, () -> IPStackUtils.getPort("70000"));
    assertEquals("Port number out of range (0-65535).", e1.getMessage());

    // Test case: negative port number
    IllegalArgumentException e2 =
        assertThrows(IllegalArgumentException.class, () -> IPStackUtils.getPort("-1"));
    assertEquals("Port number out of range (0-65535).", e2.getMessage());

    // Test case: non-numeric port
    IllegalArgumentException e3 =
        assertThrows(IllegalArgumentException.class, () -> IPStackUtils.getPort("abc"));
    assertEquals("For input string: \"abc\"", e3.getMessage());
  }

  // Test cases for formatIPAddressForURL method

  @Test
  public void testFormatIPAddressForURLWithIPv4() {
    assertEquals("192.168.1.1", IPStackUtils.formatIPAddressForURL("192.168.1.1"));
  }

  @Test
  public void testFormatIPAddressForURLWithIPv6() {
    assertEquals("[2001:0db8::1]", IPStackUtils.formatIPAddressForURL("2001:0db8::1"));
  }

  @Test
  public void testFormatIPAddressForURLWithHostname() {
    assertEquals("example.com", IPStackUtils.formatIPAddressForURL("example.com"));
  }

  @Test
  public void testFormatIPAddressForURLWithAlreadyBracketedIPv6() {
    assertEquals("[2001:0db8::1]", IPStackUtils.formatIPAddressForURL("[2001:0db8::1]"));
  }
}
