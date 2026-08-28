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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class DynConstructorsTest {

  // Prefix of the IllegalArgumentException that Constructor.newInstance throws when the argument
  // count does not fit the constructor. Matched as a prefix because the JDK 18+ accessor appends
  // ": 3 expected: 2"; the legacy accessor stops at the prefix, and this build requests it with
  // -Djdk.reflect.useDirectMethodHandle=false.
  private static final String WRONG_NUMBER_OF_ARGUMENTS = "wrong number of arguments";

  public static class VarargsHolder {
    final String first;
    final Object[] rest;

    public VarargsHolder(String first, Object... rest) {
      this.first = first;
      this.rest = rest;
    }
  }

  public static class FixedArity {
    final String value;

    public FixedArity(String value) {
      this.value = value;
    }
  }

  @Test
  public void testNewInstanceRejectsLooseVarargsArguments() {
    DynConstructors.Ctor<VarargsHolder> ctor =
        DynConstructors.builder().impl(VarargsHolder.class, String.class, Object[].class).build();

    // Truncating this call would construct successfully and drop "three", so this case pins the
    // pass-through without depending on the JDK message.
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> ctor.newInstance("one", new Object[] {"2a", "2b"}, "three"));

    assertTrue(
        thrown.getMessage().startsWith(WRONG_NUMBER_OF_ARGUMENTS),
        () -> "unexpected message: " + thrown.getMessage());

    IllegalArgumentException allLoose =
        assertThrows(IllegalArgumentException.class, () -> ctor.newInstance("one", "2a", "2b"));

    assertTrue(
        allLoose.getMessage().startsWith(WRONG_NUMBER_OF_ARGUMENTS),
        () -> "unexpected message: " + allLoose.getMessage());

    assertThrows(IllegalArgumentException.class, () -> ctor.newInstance("one"));
  }

  @Test
  public void testInvokeRejectsNonNullTarget() {
    DynConstructors.Ctor<VarargsHolder> ctor =
        DynConstructors.builder().impl(VarargsHolder.class, String.class, Object[].class).build();
    Object[] rest = new Object[] {"2a", "2b"};

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class, () -> ctor.invoke(new Object(), "one", (Object) rest));

    assertEquals("Invalid call to constructor: target must be null", thrown.getMessage());

    assertThrows(
        IllegalArgumentException.class,
        () -> ctor.invokeChecked(new Object(), "one", (Object) rest));
  }

  @Test
  public void testPackedVarargsArrayReachesConstructorThroughEveryEntryPoint() {
    DynConstructors.Ctor<VarargsHolder> ctor =
        DynConstructors.builder().impl(VarargsHolder.class, String.class, Object[].class).build();
    // Constructor.newInstance does not pack loose varargs arguments; callers pass the
    // packed array itself as the last argument.
    Object[] rest = new Object[] {"2a", "2b"};

    VarargsHolder viaNewInstance = ctor.newInstance("one", (Object) rest);
    assertEquals("one", viaNewInstance.first);
    assertArrayEquals(rest, viaNewInstance.rest);

    VarargsHolder viaNewInstanceChecked =
        assertDoesNotThrow(() -> ctor.newInstanceChecked("one", (Object) rest));
    assertArrayEquals(rest, viaNewInstanceChecked.rest);

    VarargsHolder viaInvoke = ctor.invoke(null, "one", (Object) rest);
    assertArrayEquals(rest, viaInvoke.rest);

    VarargsHolder viaInvokeChecked =
        assertDoesNotThrow(() -> ctor.invokeChecked(null, "one", (Object) rest));
    assertArrayEquals(rest, viaInvokeChecked.rest);
  }

  @Test
  public void testNewInstanceTruncatesExtraArgsForFixedArityConstructor() {
    DynConstructors.Ctor<FixedArity> ctor =
        DynConstructors.builder().impl(FixedArity.class, String.class).build();

    FixedArity fixedArity = ctor.newInstance("value", "ignored");

    assertEquals("value", fixedArity.value);
  }

  @Test
  public void testNullArgumentTypeIsAnOrdinaryMiss() {
    // DynClasses.orNull() hands back null for a class the running dependency does not provide, and
    // callers pass that straight into impl, so the chain has to fall through to the next candidate
    DynConstructors.Ctor<FixedArity> ctor =
        DynConstructors.builder()
            .impl(FixedArity.class, (Class<?>) null)
            .impl(FixedArity.class, String.class)
            .build();

    assertEquals("value", ctor.newInstance("value").value);

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> DynConstructors.builder().impl(FixedArity.class, (Class<?>) null).build());

    assertTrue(
        thrown.getMessage().contains("Missing " + FixedArity.class.getName() + "(null)"),
        () -> "unexpected message: " + thrown.getMessage());

    // getConstructor reads a null array as no arguments at all, so the candidate name has to as
    // well
    RuntimeException noArgs =
        assertThrows(
            RuntimeException.class,
            () -> DynConstructors.builder().impl(FixedArity.class, (Class<?>[]) null).build());

    assertTrue(
        noArgs.getMessage().contains("Missing " + FixedArity.class.getName() + "()"),
        () -> "unexpected message: " + noArgs.getMessage());
  }
}
