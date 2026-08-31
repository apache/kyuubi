/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kyuubi.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import org.junit.jupiter.api.Test;

public class JavaUtilsTest {

  @Test
  public void testFindLocalInetAddress() throws UnknownHostException, SocketException {
    InetAddress localHost = InetAddress.getLocalHost();
    InetAddress resolved = JavaUtils.findLocalInetAddress();
    if (localHost.isLoopbackAddress() && !resolved.isLoopbackAddress()) {
      // a replacement was scanned off a network interface, so it must belong to one, and the
      // filter that picked it rules out link-local addresses
      assertNotNull(
          NetworkInterface.getByInetAddress(resolved), () -> resolved + " is on no interface");
      assertFalse(resolved.isLinkLocalAddress(), () -> resolved + " is link-local");
    } else {
      assertEquals(localHost, resolved);
      if (localHost.isLoopbackAddress()) {
        // handing back the address it started from is only correct when the scan found nothing
        assertFalse(
            hasUsableInterfaceAddress(),
            () -> "returned " + resolved + " while an interface had a usable address");
      }
    }
  }

  // Mirrors the candidate filter in JavaUtils.findLocalInetAddress. Whoever changes that predicate
  // has to change this one too, otherwise the assertion above blames the scan for the difference.
  private static boolean hasUsableInterfaceAddress() throws SocketException {
    return Collections.list(NetworkInterface.getNetworkInterfaces()).stream()
        .flatMap(ni -> Collections.list(ni.getInetAddresses()).stream())
        .anyMatch(addr -> !addr.isLinkLocalAddress() && !addr.isLoopbackAddress());
  }
}
