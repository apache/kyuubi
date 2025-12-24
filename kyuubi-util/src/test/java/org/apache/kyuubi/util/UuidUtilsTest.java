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

import static org.junit.Assert.assertEquals;

import java.util.UUID;
import org.junit.Test;

public class UuidUtilsTest {

  @Test
  public void generateUUIDv7() {
    UUID uuid = UuidUtils.generateUUIDv7();
    assertEquals(7, uuid.version());
    assertEquals(2, uuid.variant());

    // 48-bit long
    long value = 0xFEDCBA987654L;
    uuid = UuidUtils.generateUUIDv7(value);
    assertEquals(7, uuid.version());
    assertEquals(2, uuid.variant());
  }

  @Test(expected = IllegalArgumentException.class)
  public void generateUUIDv7NegativeTimestamp() {
    long value = -0xFEDCBA987654L;
    UuidUtils.generateUUIDv7(value);
  }

  @Test(expected = IllegalArgumentException.class)
  public void generateUUIDv7GreaterThan48BitsTimestamp() {
    long value = 1L << 48;
    UuidUtils.generateUUIDv7(value);
  }
}
