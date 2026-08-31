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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.Test;

public class UuidUtilsTest {

  @Test
  public void generateUUIDv7() {
    long before = System.currentTimeMillis();
    UUID uuid = UuidUtils.generateUUIDv7();
    long after = System.currentTimeMillis();
    assertEquals(7, uuid.version());
    assertEquals(2, uuid.variant());
    // the no-arg overload stamps the current time into the top 48 bits
    long timestamp = uuid.getMostSignificantBits() >>> 16;
    assertTrue(
        before <= timestamp && timestamp <= after,
        () -> "timestamp " + timestamp + " outside [" + before + ", " + after + "]");
  }

  @Test
  public void generateUUIDv7ExplicitTimestamp() {
    // bit 47 is set, so the most significant half is negative once shifted and the decode below
    // has to be an unsigned shift
    long value = 0xFEDCBA987654L;
    UUID uuid = UuidUtils.generateUUIDv7(value);
    assertEquals(7, uuid.version());
    assertEquals(2, uuid.variant());
    assertEquals(value, uuid.getMostSignificantBits() >>> 16);
    // rand_b must vary even when the timestamp does not
    assertNotEquals(
        uuid.getLeastSignificantBits(), UuidUtils.generateUUIDv7(value).getLeastSignificantBits());
  }

  @Test
  public void generateUUIDv7NegativeTimestamp() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          long value = -0xFEDCBA987654L;
          UuidUtils.generateUUIDv7(value);
        });
  }

  @Test
  public void generateUUIDv7GreaterThan48BitsTimestamp() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          long value = 1L << 48;
          UuidUtils.generateUUIDv7(value);
        });
  }
}
