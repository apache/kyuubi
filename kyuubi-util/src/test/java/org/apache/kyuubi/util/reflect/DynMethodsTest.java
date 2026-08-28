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

package org.apache.kyuubi.util.reflect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import org.junit.jupiter.api.Test;

public class DynMethodsTest {

  public static class ReflectionTarget {

    final IllegalStateException causelessFailure = new IllegalStateException();

    public Object throwCheckedWithoutCause() throws IOException {
      throw new IOException("checked failure");
    }

    public Object throwCheckedWithRuntimeCause() throws IOException {
      throw new IOException("checked failure", new IllegalArgumentException("root"));
    }

    public Object throwUncheckedWithCause() {
      throw new IllegalStateException("unchecked failure", new IllegalArgumentException("cause"));
    }

    public Object throwCauselessUnchecked() {
      throw causelessFailure;
    }

    public static Object throwStaticUnchecked() {
      throw new IllegalStateException(
          "static failure", new IllegalArgumentException("static cause"));
    }

    public String echo(String input) {
      return input;
    }

    public static String staticEcho(String input) {
      return input;
    }

    public String concat(String... parts) {
      return String.join("", parts);
    }

    public String join(String first, String second) {
      return first + "/" + second;
    }
  }

  @Test
  public void testInvokePreservesCheckedExceptionThrownByTarget() {
    DynMethods.UnboundMethod method =
        DynMethods.builder("throwCheckedWithoutCause").impl(ReflectionTarget.class).build();

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> method.invoke(new ReflectionTarget()));

    assertTrue(thrown.getCause() instanceof IOException);
    assertEquals("checked failure", thrown.getCause().getMessage());
  }

  @Test
  public void testInvokeKeepsCheckedExceptionCarryingRuntimeCause() {
    DynMethods.UnboundMethod method =
        DynMethods.builder("throwCheckedWithRuntimeCause").impl(ReflectionTarget.class).build();

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> method.invoke(new ReflectionTarget()));

    assertEquals(RuntimeException.class, thrown.getClass());
    assertTrue(thrown.getCause() instanceof IOException);
    assertEquals("checked failure", thrown.getCause().getMessage());
    assertTrue(thrown.getCause().getCause() instanceof IllegalArgumentException);
  }

  @Test
  public void testInvokeRethrowsRuntimeExceptionFromTargetAsIs() {
    DynMethods.UnboundMethod method =
        DynMethods.builder("throwUncheckedWithCause").impl(ReflectionTarget.class).build();

    IllegalStateException thrown =
        assertThrows(IllegalStateException.class, () -> method.invoke(new ReflectionTarget()));

    assertEquals("unchecked failure", thrown.getMessage());
    assertTrue(thrown.getCause() instanceof IllegalArgumentException);
    assertEquals("cause", thrown.getCause().getMessage());
  }

  @Test
  public void testInvokeRethrowsCauselessRuntimeExceptionFromTargetAsIs() {
    ReflectionTarget target = new ReflectionTarget();
    DynMethods.UnboundMethod method =
        DynMethods.builder("throwCauselessUnchecked").impl(ReflectionTarget.class).build();

    IllegalStateException thrown =
        assertThrows(IllegalStateException.class, () -> method.invoke(target));

    assertSame(target.causelessFailure, thrown);
  }

  @Test
  public void testBoundMethodInvokeRethrowsRuntimeExceptionFromTargetAsIs() {
    ReflectionTarget target = new ReflectionTarget();
    DynMethods.BoundMethod bound =
        DynMethods.builder("throwCauselessUnchecked").impl(ReflectionTarget.class).build(target);

    IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> bound.invoke());

    assertSame(target.causelessFailure, thrown);
  }

  @Test
  public void testStaticMethodInvokeRethrowsRuntimeExceptionFromTargetAsIs() {
    DynMethods.StaticMethod staticMethod =
        DynMethods.builder("throwStaticUnchecked").impl(ReflectionTarget.class).buildStatic();

    IllegalStateException thrown =
        assertThrows(IllegalStateException.class, () -> staticMethod.invoke());

    assertEquals("static failure", thrown.getMessage());
    assertTrue(thrown.getCause() instanceof IllegalArgumentException);
    assertEquals("static cause", thrown.getCause().getMessage());
  }

  @Test
  public void testWrapperInvokeReturnsTargetResult() {
    ReflectionTarget target = new ReflectionTarget();
    DynMethods.BoundMethod bound =
        DynMethods.builder("echo").impl(ReflectionTarget.class, String.class).build(target);
    assertEquals("bound", bound.invoke("bound"));

    DynMethods.StaticMethod staticMethod =
        DynMethods.builder("staticEcho").impl(ReflectionTarget.class, String.class).buildStatic();
    assertEquals("static", staticMethod.invoke("static"));
  }

  @Test
  public void testInvokeReturnsTargetResult() {
    DynMethods.UnboundMethod fixedArity =
        DynMethods.builder("echo").impl(ReflectionTarget.class, String.class).build();
    assertEquals("arg", fixedArity.invoke(new ReflectionTarget(), "arg"));

    DynMethods.UnboundMethod varargs =
        DynMethods.builder("concat").impl(ReflectionTarget.class, String[].class).build();
    // Method.invoke does not spread-pack loose varargs arguments, so callers pass the
    // packed array as the single argument.
    assertEquals("ab", varargs.invoke(new ReflectionTarget(), (Object) new String[] {"a", "b"}));
  }

  @Test
  public void testInvokeCheckedThrowsTargetExceptionWithoutWrapping() {
    DynMethods.UnboundMethod method =
        DynMethods.builder("throwCheckedWithoutCause").impl(ReflectionTarget.class).build();

    IOException thrown =
        assertThrows(IOException.class, () -> method.invokeChecked(new ReflectionTarget()));

    assertEquals("checked failure", thrown.getMessage());
  }

  @Test
  public void testInvokeDropsExtraArgumentsForFixedArity() {
    DynMethods.UnboundMethod join =
        DynMethods.builder("join").impl(ReflectionTarget.class, String.class, String.class).build();

    // HiveConnectorUtils depends on this: it invokes the 6-arg CatalogStorageFormat.apply with
    // 7 arguments on Spark below 4.2 and lets the trailing serdeName be dropped.
    assertEquals("a/b", join.invoke(new ReflectionTarget(), "a", "b", "dropped"));
  }

  @Test
  public void testInvokePadsMissingArgumentsWithNull() {
    DynMethods.UnboundMethod join =
        DynMethods.builder("join").impl(ReflectionTarget.class, String.class, String.class).build();

    // A short argument list reaches the target with nulls rather than being rejected, which is
    // why invokeChecked skips the copy only at equal arity.
    assertEquals("a/null", join.invoke(new ReflectionTarget(), "a"));
  }

  @Test
  public void testCtorImplUsesTheConfiguredLoader() throws Exception {
    // A bootstrap-parented loader can reach no application class on any JDK, so a lookup that
    // falls back to the context loader fails and one through the configured loader succeeds.
    // Both halves set the context loader explicitly rather than relying on the ambient one.
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader blinded = new URLClassLoader(new URL[0], null)) {
      try {
        Thread.currentThread().setContextClassLoader(blinded);
        DynMethods.UnboundMethod ctor =
            DynMethods.builder("newInstance")
                .loader(getClass().getClassLoader())
                .ctorImpl(ReflectionTarget.class.getName())
                .buildChecked();

        assertEquals(ReflectionTarget.class, ctor.invoke(null).getClass());

        // The configured loader stays authoritative even when the context loader can see the
        // class: no fallback is allowed once the builder's loader cannot resolve it.
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
        NoSuchMethodException thrown =
            assertThrows(
                NoSuchMethodException.class,
                () ->
                    DynMethods.builder("newInstance")
                        .loader(blinded)
                        .ctorImpl(ReflectionTarget.class.getName())
                        .buildChecked());
        assertTrue(thrown.getMessage().contains("Cannot find method"));
      } finally {
        Thread.currentThread().setContextClassLoader(original);
      }
    }
  }
}
