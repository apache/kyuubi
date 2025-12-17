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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.SecureRandom;
import java.util.UUID;

// Inspired by Apache Iceberg, see https://github.com/apache/iceberg/pull/14700 for details.
public class UuidUtils {

  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  public static UUID generateUuidV7() {
    long epochMs = System.currentTimeMillis();
    return generateUuidV7(epochMs);
  }

  /**
   * Generate a RFC 9562 UUIDv7.
   *
   * <p>Layout: - 48-bit Unix epoch milliseconds - 4-bit version (0b0111) - 12-bit random (rand_a) -
   * 2-bit variant (RFC 4122, 0b10) - 62-bit random (rand_b)
   *
   * @param epochMs the number of milliseconds since midnight 1 Jan 1970 UTC, leap seconds excluded.
   * @return a {@code UUID} constructed using the given {@code epochMs}
   * @throws IllegalArgumentException if epochMs is negative or greater than {@code (1L << 48) - 1}
   */
  public static UUID generateUuidV7(long epochMs) {
    if ((epochMs >> 48) != 0) {
      throw new IllegalArgumentException(
          "Invalid timestamp: does not fit within 48 bits: " + epochMs);
    }
    // Draw 10 random bytes once: 2 bytes for rand_a (12 bits) and 8 bytes for rand_b (62 bits)
    byte[] randomBytes = new byte[10];
    SECURE_RANDOM.nextBytes(randomBytes);
    ByteBuffer rb = ByteBuffer.wrap(randomBytes).order(ByteOrder.BIG_ENDIAN);
    long randMSB = ((long) rb.getShort()) & 0x0FFFL; // 12 bits
    long randLSB = rb.getLong() & 0x3FFFFFFFFFFFFFFFL; // 62 bits

    long msb = (epochMs << 16); // place timestamp in the top 48 bits
    msb |= 0x7000L; // version 7 (UUID bits 48..51)
    msb |= randMSB; // low 12 bits of MSB

    long lsb = 0x8000000000000000L; // RFC 4122 variant '10'
    lsb |= randLSB;

    return new UUID(msb, lsb);
  }
}
