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

/**
 * Utility methods for handling IPv4/IPv6 host:port strings. The logic mirrors Hive's {@code
 * org.apache.hadoop.hive.common.IPStackUtils}.
 */
public final class IPStackUtils {

  private IPStackUtils() {}

  /**
   * Splits a given input string representing a Hostname or an IP address and port into an
   * `HostPort` object. The input string must be in the format of IPv4/IPv6/[IPv6]/hostname:port.
   *
   * @param input The input string containing the Hostname/IP address and port, in the format
   *     "IPv4:port", "[IPv6]:port", "IPv6:port", or "hostname:port".
   * @return A {@link HostPort} object containing the parsed IP address and port number.
   * @throws IllegalArgumentException If the input format is invalid, if the host is null or empty,
   *     or if the port number is invalid.
   */
  public static HostPort getHostAndPort(String input) {
    String host;
    int port;

    if (isEmpty(input)) {
      throw new IllegalArgumentException("Input string is null or empty");
    }

    // Check if the input contains a colon, which separates the host and port
    int colonIndex = input.lastIndexOf(':');
    if (colonIndex == -1) {
      throw new IllegalArgumentException("Input does not contain a port.");
    }

    // Extract the host and port parts
    host = input.substring(0, colonIndex);
    port = getPort(input.substring(colonIndex + 1));

    // Check if the host is not null or empty
    validateHostNotEmpty(host);

    // Handle IPv6 addresses enclosed in square brackets (e.g., [IPv6]:port)
    if (host.startsWith("[") && host.endsWith("]")) {
      host = host.substring(1, host.length() - 1); // Remove the square brackets
    }

    return new HostPort(host, port);
  }

  /**
   * Returns an integer representation of the port number. Also validates whether the given string
   * represents a valid port number. A valid port number is an integer between 0 and 65535
   * inclusive.
   *
   * @param portString The string representing the port number.
   * @return {@code int} the port number.
   */
  public static int getPort(String portString) {
    if (isEmpty(portString)) {
      throw new IllegalArgumentException("port is null or empty");
    }

    int port = Integer.parseInt(portString);
    validatePort(port);
    return port;
  }

  private static void validateHostNotEmpty(String host) {
    if (isEmpty(host) || host.equals("[]")) {
      throw new IllegalArgumentException("Host address is null or empty.");
    }
  }

  private static void validatePort(int port) {
    if (port < 0 || port > 65535) {
      throw new IllegalArgumentException("Port number out of range (0-65535).");
    }
  }

  private static boolean isEmpty(String s) {
    return s == null || s.isEmpty();
  }

  /**
   * Prepares an IP address for use in a URL.
   *
   * <p>This method ensures that IPv6 addresses are enclosed in square brackets, as required by URL
   * syntax. IPv4 addresses and hostnames remain unchanged.
   *
   * @param ipAddress the IP address or hostname to format
   * @return the formatted IP address for use in a URL
   */
  public static String formatIPAddressForURL(String ipAddress) {
    if (ipAddress.contains(":") && !ipAddress.startsWith("[") && !ipAddress.endsWith("]")) {
      // IPv6 address
      return "[" + ipAddress + "]";
    } else {
      // IPv4 address or hostname
      return ipAddress;
    }
  }

  public static class HostPort {

    private final String hostname;
    private final int port;

    public HostPort(String hostname, int port) {
      this.hostname = hostname;
      this.port = port;
    }

    public String getHostname() {
      return hostname;
    }

    public int getPort() {
      return port;
    }
  }
}
