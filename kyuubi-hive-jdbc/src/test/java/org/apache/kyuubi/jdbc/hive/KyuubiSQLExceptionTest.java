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
package org.apache.kyuubi.jdbc.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Collections;
import org.junit.jupiter.api.Test;

public class KyuubiSQLExceptionTest {

  private static boolean remoteClassInitialized;

  public static class StaticInitializerProbe {
    static {
      remoteClassInitialized = true;
    }

    public StaticInitializerProbe(String message) {}
  }

  @Test
  public void decodeOnlyThrowableClasses() {
    String className = StaticInitializerProbe.class.getName();
    Throwable cause =
        KyuubiSQLException.toCause(
            Collections.singletonList("*" + className + ":remote failure:0:-1"));

    assertFalse(remoteClassInitialized);
    assertEquals(RuntimeException.class, cause.getClass());
    assertEquals(className + ":remote failure", cause.getMessage());

    IllegalArgumentException original = new IllegalArgumentException("original message");
    Throwable restored = KyuubiSQLException.toCause(KyuubiSQLException.toString(original));
    assertEquals(original.getClass(), restored.getClass());
    assertEquals(original.getMessage(), restored.getMessage());
  }
}
