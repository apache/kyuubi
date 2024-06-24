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

import java.io.File;
import java.net.*;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaUtils {

  private static final Logger LOG = LoggerFactory.getLogger(JavaUtils.class);

  /** Whether the underlying operating system is Windows. */
  public static final boolean isWindows = System.getProperty("os.name", "").startsWith("Windows");

  /** Whether the underlying operating system is MacOS. */
  public static final boolean isMac = System.getProperty("os.name", "").startsWith("Mac");

  public static String getCodeSourceLocation(Class<?> clazz) {
    try {
      return new File(clazz.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
    } catch (URISyntaxException rethrow) {
      throw new RuntimeException(rethrow);
    }
  }

  public static InetAddress findLocalInetAddress() throws UnknownHostException, SocketException {
    InetAddress address = InetAddress.getLocalHost();
    if (address.isLoopbackAddress()) {
      List<NetworkInterface> activeNetworkIFs =
          Collections.list(NetworkInterface.getNetworkInterfaces());
      if (!isWindows) {
        Collections.reverse(activeNetworkIFs);
      }

      for (NetworkInterface ni : activeNetworkIFs) {
        List<InetAddress> addresses =
            Collections.list(ni.getInetAddresses()).stream()
                .filter(addr -> !addr.isLinkLocalAddress() && !addr.isLoopbackAddress())
                .collect(Collectors.toList());

        if (!addresses.isEmpty()) {
          InetAddress addr =
              addresses.stream()
                  .filter(a -> a instanceof Inet4Address)
                  .findFirst()
                  .orElse(addresses.get(0));

          InetAddress strippedAddress = InetAddress.getByAddress(addr.getAddress());

          // We've found an address that looks reasonable!
          LOG.warn(
              "{} was resolved to a loopback address: {}, using {}",
              addr.getHostName(),
              addr.getHostAddress(),
              strippedAddress.getHostAddress());
          return strippedAddress;
        }
      }

      LOG.warn(
          "{} was resolved to a loopback address: {} but we couldn't find any external IP address!",
          address.getHostName(),
          address.getHostAddress());
    }
    return address;
  }
}
