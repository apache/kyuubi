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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import javax.security.auth.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class that adapts Security Manager APIs across JDK versions (8 through 25+).
 *
 * <p>In JDK 17 the Security Manager and related APIs were deprecated for removal (JEP 411). In JDK
 * 24 the Security Manager was permanently disabled (JEP 486). This class bridges {@code
 * Subject.getSubject()}, {@code Subject.doAs()}, and {@code AccessController.getContext()} to their
 * JDK 18+ replacements ({@code Subject.current()} and {@code Subject.callAs()}) using {@code
 * MethodHandle} lookups resolved once at class load time.
 *
 * <p>Ported from Apache Hadoop 3.5.1 ({@code
 * org.apache.hadoop.security.authentication.util.SubjectUtil}), HADOOP-19212 and HADOOP-19906,
 * which is itself derived from Apache Calcite Avatica and the Jetty implementation.
 */
public final class SubjectUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SubjectUtil.class);

  private static final MethodHandle CALL_AS = lookupCallAs();
  static final boolean HAS_CALL_AS = CALL_AS != null;
  private static final MethodHandle DO_AS = HAS_CALL_AS ? null : lookupDoAs();
  private static final MethodHandle DO_AS_THROW_EXCEPTION =
      HAS_CALL_AS ? null : lookupDoAsThrowException();
  private static final MethodHandle CURRENT = lookupCurrent();

  // "1.8" -> 8, "9" -> 9, "17" -> 17, etc.
  private static final int JAVA_SPEC_VER =
      Math.max(
          8, Integer.parseInt(System.getProperty("java.specification.version").split("\\.")[0]));

  /** True if the current JVM copies the current JAAS subject into new threads automatically. */
  public static final boolean THREAD_INHERITS_SUBJECT = checkThreadInheritsSubject();

  /**
   * Kyuubi-managed {@link InheritableThreadLocal} mirroring the active Subject for the duration of
   * {@link #callAs} / {@link #doAs}. On JDK 22+ the JVM stopped propagating the Subject to newly
   * constructed threads via {@code AccessControlContext} (JEP 411 / 486), and the new {@code
   * ScopedValue}-based mechanism behind {@code Subject.current()} also does not inherit across
   * {@code Thread.<init>}. Only consulted when {@link #THREAD_INHERITS_SUBJECT} is {@code false}.
   */
  private static final InheritableThreadLocal<Subject> CURRENT_SUBJECT_TL =
      THREAD_INHERITS_SUBJECT ? null : new InheritableThreadLocal<>();

  private static MethodHandle lookupCallAs() {
    try {
      return MethodHandles.lookup()
          .findStatic(
              Subject.class,
              "callAs",
              MethodType.methodType(Object.class, Subject.class, Callable.class));
    } catch (NoSuchMethodException x) {
      return null;
    } catch (IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static boolean checkThreadInheritsSubject() {
    // JDK 22+ stopped propagating Subject to new threads via AccessControlContext.
    // We accept the minor overhead for EOL 22/23 to avoid SecurityManager console warnings.
    return JAVA_SPEC_VER <= 21;
  }

  private static MethodHandle lookupDoAs() {
    try {
      return MethodHandles.lookup()
          .findStatic(
              Subject.class,
              "doAs",
              MethodType.methodType(Object.class, Subject.class, PrivilegedAction.class));
    } catch (IllegalAccessException | NoSuchMethodException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle lookupDoAsThrowException() {
    try {
      return MethodHandles.lookup()
          .findStatic(
              Subject.class,
              "doAs",
              MethodType.methodType(Object.class, Subject.class, PrivilegedExceptionAction.class));
    } catch (IllegalAccessException | NoSuchMethodException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle lookupCurrent() {
    try {
      return MethodHandles.lookup()
          .findStatic(Subject.class, "current", MethodType.methodType(Subject.class));
    } catch (NoSuchMethodException e) {
      // JDK < 18: compose AccessController.getContext() -> Subject.getSubject()
      MethodHandle getContext = lookupGetContext();
      MethodHandle getSubject = lookupGetSubject();
      return MethodHandles.filterReturnValue(getContext, getSubject);
    } catch (IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle lookupGetSubject() {
    try {
      Class<?> contextKlass =
          ClassLoader.getSystemClassLoader().loadClass("java.security.AccessControlContext");
      return MethodHandles.lookup()
          .findStatic(
              Subject.class, "getSubject", MethodType.methodType(Subject.class, contextKlass));
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle lookupGetContext() {
    try {
      Class<?> controllerKlass =
          ClassLoader.getSystemClassLoader().loadClass("java.security.AccessController");
      Class<?> contextKlass =
          ClassLoader.getSystemClassLoader().loadClass("java.security.AccessControlContext");
      return MethodHandles.lookup()
          .findStatic(controllerKlass, "getContext", MethodType.methodType(contextKlass));
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Runs {@code action} as {@code subject}, bridging to {@code Subject.callAs()} on JDK 18+ and
   * falling back to {@code Subject.doAs()} on older JDKs. On JDK 22+ also maintains {@link
   * #CURRENT_SUBJECT_TL} so platform threads constructed inside {@code action} inherit the subject.
   *
   * @throws CompletionException wrapping any exception thrown by {@code action}
   */
  public static <T> T callAs(Subject subject, Callable<T> action) throws CompletionException {
    Objects.requireNonNull(action);
    if (THREAD_INHERITS_SUBJECT) {
      return invokeCallAs(subject, action);
    }
    final Subject prev = CURRENT_SUBJECT_TL.get();
    if (subject == null) {
      CURRENT_SUBJECT_TL.remove();
    } else if (subject != prev) {
      CURRENT_SUBJECT_TL.set(subject);
    }
    try {
      return invokeCallAs(subject, action);
    } finally {
      if (prev == null) {
        CURRENT_SUBJECT_TL.remove();
      } else if (prev != subject) {
        CURRENT_SUBJECT_TL.set(prev);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T invokeCallAs(Subject subject, Callable<T> action)
      throws CompletionException {
    if (HAS_CALL_AS) {
      try {
        return (T) CALL_AS.invoke(subject, action);
      } catch (Throwable t) {
        throw sneakyThrow(t);
      }
    } else {
      try {
        return doAs(subject, callableToPrivilegedAction(action));
      } catch (Exception e) {
        throw new CompletionException(e);
      }
    }
  }

  /** Bridges to {@code Subject.callAs()} on JDK 18+ or {@code Subject.doAs()} on older JDKs. */
  @SuppressWarnings("unchecked")
  public static <T> T doAs(Subject subject, PrivilegedAction<T> action) {
    Objects.requireNonNull(action);
    if (HAS_CALL_AS) {
      try {
        return callAs(subject, privilegedActionToCallable(action));
      } catch (CompletionException ce) {
        Throwable cause = ce.getCause();
        throw sneakyThrow(cause != null ? cause : ce);
      }
    } else {
      try {
        return (T) DO_AS.invoke(subject, action);
      } catch (Throwable t) {
        throw sneakyThrow(t);
      }
    }
  }

  /** Bridges to {@code Subject.callAs()} on JDK 18+ or {@code Subject.doAs()} on older JDKs. */
  @SuppressWarnings("unchecked")
  public static <T> T doAs(Subject subject, PrivilegedExceptionAction<T> action)
      throws PrivilegedActionException {
    Objects.requireNonNull(action);
    if (HAS_CALL_AS) {
      try {
        return callAs(subject, privilegedExceptionActionToCallable(action));
      } catch (CompletionException ce) {
        Throwable cause = ce.getCause();
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        } else if (cause instanceof Exception) {
          throw new PrivilegedActionException((Exception) cause);
        } else {
          throw sneakyThrow(cause);
        }
      }
    } else {
      try {
        return (T) DO_AS_THROW_EXCEPTION.invoke(subject, action);
      } catch (Throwable t) {
        throw sneakyThrow(t);
      }
    }
  }

  /**
   * Returns the {@link Subject} associated with the current execution context, or {@code null} if
   * there is none. On JDK 18+ delegates to {@code Subject.current()}; on older JDKs delegates to
   * {@code Subject.getSubject(AccessController.getContext())}. On JDK 22+ also consults {@link
   * #CURRENT_SUBJECT_TL} to restore subject visibility for inherited platform threads.
   */
  public static Subject current() {
    if (!THREAD_INHERITS_SUBJECT) {
      Subject fromJdk = invokeJdkCurrent();
      if (fromJdk != null) {
        LOG.trace("Get current Subject from JDK API directly");
        return fromJdk;
      }
      LOG.trace("Get current Subject from Kyuubi-managed InheritableThreadLocal");
      return CURRENT_SUBJECT_TL.get();
    }
    return invokeJdkCurrent();
  }

  private static Subject invokeJdkCurrent() {
    try {
      return (Subject) CURRENT.invoke();
    } catch (Throwable t) {
      throw sneakyThrow(t);
    }
  }

  private static <T> PrivilegedAction<T> callableToPrivilegedAction(Callable<T> callable) {
    return () -> {
      try {
        return callable.call();
      } catch (Exception e) {
        throw sneakyThrow(e);
      }
    };
  }

  private static <T> Callable<T> privilegedExceptionActionToCallable(
      PrivilegedExceptionAction<T> action) {
    return action::run;
  }

  private static <T> Callable<T> privilegedActionToCallable(PrivilegedAction<T> action) {
    return action::run;
  }

  @SuppressWarnings("unchecked")
  static <E extends Throwable> RuntimeException sneakyThrow(Throwable e) throws E {
    throw (E) e;
  }

  private SubjectUtil() {}
}
