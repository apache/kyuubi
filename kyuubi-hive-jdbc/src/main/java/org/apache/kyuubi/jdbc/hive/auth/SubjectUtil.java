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

package org.apache.kyuubi.jdbc.hive.auth;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.security.AccessController;
import javax.security.auth.Subject;

/**
 * Bridge for {@code Subject.getSubject()} across JDK versions.
 *
 * <p>JDK 18 introduced {@code Subject.current()} as a replacement for {@code
 * Subject.getSubject(AccessController.getContext())}. JDK 24+ made {@code
 * AccessController.getContext()} throw, and JDK 25 made {@code Subject.getSubject()} throw. This
 * class resolves the new API once at class load time and falls back to the old API on JDK &lt; 18.
 */
public final class SubjectUtil {

  // Resolved at class load time; null on JDK < 18.
  private static final MethodHandle SUBJECT_CURRENT;

  static {
    MethodHandle handle = null;
    try {
      handle =
          MethodHandles.lookup()
              .findStatic(Subject.class, "current", MethodType.methodType(Subject.class));
    } catch (NoSuchMethodException | IllegalAccessException ignored) {
      // JDK < 18: Subject.current() does not exist; fall back to getSubject().
    }
    SUBJECT_CURRENT = handle;
  }

  private SubjectUtil() {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the {@link Subject} associated with the current execution context, or {@code null} if
   * there is none.
   *
   * <p>On JDK 18+ delegates to {@code Subject.current()}. On older JDKs delegates to {@code
   * Subject.getSubject(AccessController.getContext())}.
   */
  @SuppressWarnings("removal")
  public static Subject current() {
    if (SUBJECT_CURRENT != null) {
      try {
        return (Subject) SUBJECT_CURRENT.invokeExact();
      } catch (Throwable t) {
        throw new RuntimeException("Failed to invoke Subject.current()", t);
      }
    }
    return Subject.getSubject(AccessController.getContext());
  }
}
