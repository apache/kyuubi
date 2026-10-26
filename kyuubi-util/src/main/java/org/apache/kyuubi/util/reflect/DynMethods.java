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

package org.apache.kyuubi.util.reflect;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Adapted from iceberg-common, which is itself derived from parquet-common. A lookup that finds no
 * implementation here reports the candidates it tried; see {@link Builder#build()}.
 */
public class DynMethods {

  private DynMethods() {}

  // The type is absent from Java 8, which this module compiles against, so it is matched
  // by name where it is caught.
  private static final String INACCESSIBLE_OBJECT_EXCEPTION =
      "java.lang.reflect.InaccessibleObjectException";

  /**
   * Convenience wrapper class around {@link Method}.
   *
   * <p>Allows callers to invoke the wrapped method with any checked Exception wrapped by
   * RuntimeException (RuntimeExceptions are rethrown as-is), or with a single Exception catch
   * block.
   */
  public static class UnboundMethod {

    private final Method method;
    private final String name;
    private final int argLength;

    UnboundMethod(Method method, String name) {
      this.method = method;
      this.name = name;
      this.argLength =
          (method == null || method.isVarArgs()) ? -1 : method.getParameterTypes().length;
    }

    @SuppressWarnings("unchecked")
    public <R> R invokeChecked(Object target, Object... args) throws Exception {
      try {
        // The copy exists only to change arity: it drops extra arguments and null-pads a
        // short list. At equal arity it changes nothing, so hand Method.invoke the caller's
        // array as the varargs branch always has. The guard has to stay ==: passing through
        // when longer would stop truncating, and copying only when longer, the way
        // DynConstructors.newInstanceChecked does, would stop padding.
        if (argLength < 0 || args.length == argLength) {
          return (R) method.invoke(target, args);
        } else {
          return (R) method.invoke(target, Arrays.copyOfRange(args, 0, argLength));
        }

      } catch (InvocationTargetException e) {
        if (e.getCause() instanceof Exception) {
          throw (Exception) e.getCause();
        }
        if (e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        }
        throw new RuntimeException(e.getCause());
      }
    }

    public <R> R invoke(Object target, Object... args) {
      try {
        return this.invokeChecked(target, args);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Returns this method as a BoundMethod for the given receiver.
     *
     * @param receiver an Object to receive the method invocation
     * @return a {@link BoundMethod} for this method and the receiver
     * @throws IllegalStateException if the method is static
     * @throws IllegalArgumentException if the receiver's class is incompatible
     */
    public BoundMethod bind(Object receiver) {
      if (isStatic()) {
        throw new IllegalStateException("Cannot bind static method " + method.toGenericString());
      }
      if (!method.getDeclaringClass().isAssignableFrom(receiver.getClass())) {
        throw new IllegalArgumentException(
            "Cannot bind" + method.toGenericString() + "to instance of " + receiver.getClass());
      }
      return new BoundMethod(this, receiver);
    }

    /** Returns whether the method is a static method. */
    public boolean isStatic() {
      return Modifier.isStatic(method.getModifiers());
    }

    /** Returns whether the method is a noop. */
    public boolean isNoop() {
      return this == NOOP;
    }

    /**
     * Returns this method as a StaticMethod.
     *
     * @return a {@link StaticMethod} for this method
     * @throws IllegalStateException if the method is not static
     */
    public StaticMethod asStatic() {
      if (!isStatic()) {
        throw new IllegalStateException("Method is not static");
      }
      return new StaticMethod(this);
    }

    @Override
    public String toString() {
      return "DynMethods.UnboundMethod(name=" + name + " method=" + method.toGenericString() + ")";
    }

    /** Singleton {@link UnboundMethod}, performs no operation and returns null. */
    private static final UnboundMethod NOOP =
        new UnboundMethod(null, "NOOP") {
          @Override
          public <R> R invokeChecked(Object target, Object... args) throws Exception {
            return null;
          }

          @Override
          public BoundMethod bind(Object receiver) {
            return new BoundMethod(this, receiver);
          }

          @Override
          public StaticMethod asStatic() {
            return new StaticMethod(this);
          }

          @Override
          public boolean isStatic() {
            return true;
          }

          @Override
          public String toString() {
            return "DynMethods.UnboundMethod(NOOP)";
          }
        };
  }

  public static class BoundMethod {
    private final UnboundMethod method;
    private final Object receiver;

    private BoundMethod(UnboundMethod method, Object receiver) {
      this.method = method;
      this.receiver = receiver;
    }

    public <R> R invokeChecked(Object... args) throws Exception {
      return method.invokeChecked(receiver, args);
    }

    public <R> R invoke(Object... args) {
      return method.invoke(receiver, args);
    }
  }

  public static class StaticMethod {
    private final UnboundMethod method;

    private StaticMethod(UnboundMethod method) {
      this.method = method;
    }

    public <R> R invokeChecked(Object... args) throws Exception {
      return method.invokeChecked(null, args);
    }

    public <R> R invoke(Object... args) {
      return method.invoke(null, args);
    }
  }

  /**
   * Constructs a new builder for calling methods dynamically.
   *
   * @param methodName name of the method the builder will locate
   * @return a Builder for finding a method
   */
  public static Builder builder(String methodName) {
    return new Builder(methodName);
  }

  public static class Builder {
    private final String name;
    private ClassLoader loader = Thread.currentThread().getContextClassLoader();
    private UnboundMethod method = null;
    // One entry per lookup that missed, in the order the lookups ran. A list rather than the map
    // DynConstructors keys by candidate name: two lookups can render the same name and still miss
    // for unrelated reasons, and dropping either of them is what this is here to avoid.
    private final List<Miss> misses = new ArrayList<>();

    public Builder(String methodName) {
      this.name = methodName;
    }

    /**
     * Set the {@link ClassLoader} used to lookup classes by name.
     *
     * <p>If not set, the current thread's ClassLoader is used.
     *
     * @param newLoader a ClassLoader
     * @return this Builder for method chaining
     */
    public Builder loader(ClassLoader newLoader) {
      this.loader = newLoader;
      return this;
    }

    /**
     * If no implementation has been found, adds a NOOP method.
     *
     * <p>Note: calls to impl will not match after this method is called!
     *
     * @return this Builder for method chaining
     */
    public Builder orNoop() {
      if (method == null) {
        this.method = UnboundMethod.NOOP;
      }
      return this;
    }

    /**
     * Checks for an implementation, first finding the given class by name.
     *
     * @param className name of a class
     * @param methodName name of a method (different from constructor)
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getMethod(String, Class[])
     */
    public Builder impl(String className, String methodName, Class<?>... argClasses) {
      // don't do any work if an implementation has been found
      if (method != null) {
        return this;
      }

      try {
        Class<?> targetClass = Class.forName(className, true, loader);
        impl(targetClass, methodName, argClasses);
      } catch (ClassNotFoundException e) {
        // not the right implementation
        misses.add(new Miss(candidateName(className, methodName, argClasses), e));
      }
      return this;
    }

    /**
     * Checks for an implementation, first finding the given class by name.
     *
     * <p>The name passed to the constructor is the method name used.
     *
     * @param className name of a class
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getMethod(String, Class[])
     */
    public Builder impl(String className, Class<?>... argClasses) {
      impl(className, name, argClasses);
      return this;
    }

    /**
     * Checks for a method implementation.
     *
     * @param targetClass a class instance
     * @param methodName name of a method (different from constructor)
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getMethod(String, Class[])
     */
    public Builder impl(Class<?> targetClass, String methodName, Class<?>... argClasses) {
      // don't do any work if an implementation has been found
      if (method != null) {
        return this;
      }

      try {
        this.method = new UnboundMethod(targetClass.getMethod(methodName, argClasses), name);
      } catch (NoSuchMethodException e) {
        // not the right implementation
        misses.add(new Miss(candidateName(targetClass.getName(), methodName, argClasses), e));
      }
      return this;
    }

    /**
     * Checks for a method implementation.
     *
     * <p>The name passed to the constructor is the method name used.
     *
     * @param targetClass a class instance
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getMethod(String, Class[])
     */
    public Builder impl(Class<?> targetClass, Class<?>... argClasses) {
      impl(targetClass, name, argClasses);
      return this;
    }

    public Builder ctorImpl(Class<?> targetClass, Class<?>... argClasses) {
      // don't do any work if an implementation has been found
      if (method != null) {
        return this;
      }

      try {
        // The delegated lookup aggregates a message of its own, which names its base class; give
        // it the one we have so the entry recorded below does not report the constructor as absent
        // from null. The overload below has no loaded class to hand over.
        this.method =
            new DynConstructors.Builder(targetClass).impl(targetClass, argClasses).buildChecked();
      } catch (NoSuchMethodException e) {
        // not the right implementation
        misses.add(new Miss(candidateName(targetClass.getName(), "<init>", argClasses), e));
      }
      return this;
    }

    /**
     * Checks for a constructor implementation, first finding the given class by name using this
     * builder's {@link ClassLoader}.
     *
     * <p>Neither upstream copy forwarded the loader to the inner constructor builder, so the name
     * resolved through the thread context loader regardless of what the caller configured.
     * iceberg-common removed {@code ctorImpl} altogether in 1.7.0 (apache/iceberg#10818).
     *
     * @param className name of a class
     * @param argClasses argument classes for the constructor
     * @return this Builder for method chaining
     * @see Class#forName(String)
     */
    public Builder ctorImpl(String className, Class<?>... argClasses) {
      // don't do any work if an implementation has been found
      if (method != null) {
        return this;
      }

      try {
        this.method =
            new DynConstructors.Builder().loader(loader).impl(className, argClasses).buildChecked();
      } catch (NoSuchMethodException e) {
        // not the right implementation
        misses.add(new Miss(candidateName(className, "<init>", argClasses), e));
      }
      return this;
    }

    /**
     * Checks for an implementation, first finding the given class by name.
     *
     * @param className name of a class
     * @param methodName name of a method (different from constructor)
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getMethod(String, Class[])
     */
    public Builder hiddenImpl(String className, String methodName, Class<?>... argClasses) {
      // don't do any work if an implementation has been found
      if (method != null) {
        return this;
      }

      try {
        Class<?> targetClass = Class.forName(className, true, loader);
        hiddenImpl(targetClass, methodName, argClasses);
      } catch (ClassNotFoundException e) {
        // not the right implementation
        misses.add(new Miss("declared " + candidateName(className, methodName, argClasses), e));
      }
      return this;
    }

    /**
     * Checks for an implementation, first finding the given class by name.
     *
     * <p>The name passed to the constructor is the method name used.
     *
     * @param className name of a class
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getMethod(String, Class[])
     */
    public Builder hiddenImpl(String className, Class<?>... argClasses) {
      hiddenImpl(className, name, argClasses);
      return this;
    }

    /**
     * Checks for a method implementation.
     *
     * <p>Neither upstream copy catches InaccessibleObjectException from {@code setAccessible}, so
     * under strong encapsulation one inaccessible method aborts the whole fallback chain. This copy
     * counts it as a miss like the other lookup failures, and records the exception with the
     * candidate so its "does not opens" text survives into the build failure.
     *
     * @param targetClass a class instance
     * @param methodName name of a method (different from constructor)
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getMethod(String, Class[])
     */
    public Builder hiddenImpl(Class<?> targetClass, String methodName, Class<?>... argClasses) {
      // don't do any work if an implementation has been found
      if (method != null) {
        return this;
      }

      try {
        Method hidden = targetClass.getDeclaredMethod(methodName, argClasses);
        AccessController.doPrivileged(new MakeAccessible(hidden));
        this.method = new UnboundMethod(hidden, name);
      } catch (SecurityException | NoSuchMethodException e) {
        // unusable or not the right implementation
        misses.add(
            new Miss(
                "declared " + candidateName(targetClass.getName(), methodName, argClasses), e));
      } catch (RuntimeException e) {
        // setAccessible on a member of a package that is not open reports
        // InaccessibleObjectException: since JDK 9 for named modules, since JDK 16 by
        // default from the unnamed module; record it as a candidate miss like the
        // failures above instead of letting it escape the fallback chain.
        if (!INACCESSIBLE_OBJECT_EXCEPTION.equals(e.getClass().getName())) {
          throw e;
        }
        misses.add(
            new Miss(
                "declared " + candidateName(targetClass.getName(), methodName, argClasses), e));
      }
      return this;
    }

    /**
     * Checks for a method implementation.
     *
     * <p>The name passed to the constructor is the method name used.
     *
     * @param targetClass a class instance
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getMethod(String, Class[])
     */
    public Builder hiddenImpl(Class<?> targetClass, Class<?>... argClasses) {
      hiddenImpl(targetClass, name, argClasses);
      return this;
    }

    /**
     * Returns the first valid implementation as a UnboundMethod or throws a RuntimeException if
     * there is none, with every recorded lookup failure listed in the message and attached as a
     * suppressed throwable.
     *
     * @return a {@link UnboundMethod} with a valid implementation
     * @throws RuntimeException if no implementation was found
     */
    public UnboundMethod build() {
      if (method != null) {
        return method;
      } else {
        throw buildRuntimeException(name, misses);
      }
    }

    /**
     * Returns the first valid implementation as a BoundMethod or throws a RuntimeException if there
     * is none.
     *
     * @param receiver an Object to receive the method invocation
     * @return a {@link BoundMethod} with a valid implementation and receiver
     * @throws IllegalStateException if the method is static
     * @throws IllegalArgumentException if the receiver's class is incompatible
     * @throws RuntimeException if no implementation was found
     */
    public BoundMethod build(Object receiver) {
      return build().bind(receiver);
    }

    /**
     * Returns the first valid implementation as a UnboundMethod or throws a NoSuchMethodException
     * if there is none, reporting the recorded lookup failures the way {@link #build()} does.
     *
     * @return a {@link UnboundMethod} with a valid implementation
     * @throws NoSuchMethodException if no implementation was found
     */
    public UnboundMethod buildChecked() throws NoSuchMethodException {
      if (method != null) {
        return method;
      } else {
        throw buildCheckedException(name, misses);
      }
    }

    /**
     * Returns the first valid implementation as a BoundMethod or throws a NoSuchMethodException if
     * there is none.
     *
     * @param receiver an Object to receive the method invocation
     * @return a {@link BoundMethod} with a valid implementation and receiver
     * @throws IllegalStateException if the method is static
     * @throws IllegalArgumentException if the receiver's class is incompatible
     * @throws NoSuchMethodException if no implementation was found
     */
    public BoundMethod buildChecked(Object receiver) throws NoSuchMethodException {
      return buildChecked().bind(receiver);
    }

    /**
     * Returns the first valid implementation as a StaticMethod or throws a NoSuchMethodException if
     * there is none.
     *
     * @return a {@link StaticMethod} with a valid implementation
     * @throws IllegalStateException if the method is not static
     * @throws NoSuchMethodException if no implementation was found
     */
    public StaticMethod buildStaticChecked() throws NoSuchMethodException {
      return buildChecked().asStatic();
    }

    /**
     * Returns the first valid implementation as a StaticMethod or throws a RuntimeException if
     * there is none.
     *
     * @return a {@link StaticMethod} with a valid implementation
     * @throws IllegalStateException if the method is not static
     * @throws RuntimeException if no implementation was found
     */
    public StaticMethod buildStatic() {
      return build().asStatic();
    }
  }

  /** A lookup that missed: what was looked for, and what came back. */
  private static final class Miss {
    private final String candidate;
    private final Throwable cause;

    Miss(String candidate, Throwable cause) {
      this.candidate = candidate;
      this.cause = cause;
    }
  }

  private static NoSuchMethodException buildCheckedException(String name, List<Miss> misses) {
    NoSuchMethodException exc =
        new NoSuchMethodException("Cannot find method: " + name + formatMisses(misses));
    misses.forEach(miss -> exc.addSuppressed(miss.cause));
    return exc;
  }

  private static RuntimeException buildRuntimeException(String name, List<Miss> misses) {
    RuntimeException exc =
        new RuntimeException("Cannot find method: " + name + formatMisses(misses));
    misses.forEach(miss -> exc.addSuppressed(miss.cause));
    return exc;
  }

  /**
   * Appends one entry per miss. Each entry carries its own leading newline, unlike the equivalent
   * in {@link DynConstructors}, so a builder that recorded nothing adds nothing.
   */
  private static String formatMisses(List<Miss> misses) {
    StringBuilder sb = new StringBuilder();
    for (Miss miss : misses) {
      sb.append("\n\tMissing ")
          .append(miss.candidate)
          .append(" [")
          .append(miss.cause.getClass().getName())
          .append(": ")
          .append(indented(miss.cause.getMessage()))
          .append("]");
    }
    return sb.toString();
  }

  /**
   * Pushes the continuation lines of a nested message in one level, so that the entries a delegated
   * lookup aggregated do not read as candidates of this builder.
   */
  private static String indented(String message) {
    return message == null ? null : message.replace("\n", "\n\t");
  }

  private static String candidateName(String className, String methodName, Class<?>... argClasses) {
    StringBuilder sb = new StringBuilder();
    sb.append(className).append("#").append(methodName).append("(");
    boolean first = true;
    // A caller probing an optional dependency passes down the null DynClasses.orNull() handed it,
    // and getMethod reads a null array as no arguments at all. Both are ordinary misses there, so
    // naming the candidate must not turn either into a throw.
    for (Class<?> argClass : argClasses == null ? new Class<?>[0] : argClasses) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      sb.append(argClass == null ? "null" : argClass.getName());
    }
    return sb.append(")").toString();
  }

  private static class MakeAccessible implements PrivilegedAction<Void> {
    private Method hidden;

    MakeAccessible(Method hidden) {
      this.hidden = hidden;
    }

    @Override
    public Void run() {
      hidden.setAccessible(true);
      return null;
    }
  }
}
