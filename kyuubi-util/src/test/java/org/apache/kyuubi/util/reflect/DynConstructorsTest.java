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

import java.math.BigInteger;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class DynConstructorsTest {

  // "1.8" -> 8, "9" -> 9, "17" -> 17, etc.
  private static final int JAVA_SPEC_VER =
      Math.max(
          8, Integer.parseInt(System.getProperty("java.specification.version").split("\\.")[0]));

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
  public void testHiddenImplHandlesStronglyEncapsulatedConstructors() throws Exception {
    // The test JVM opens java.base/java.lang to the unnamed module, so the encapsulated
    // path needs a package the surefire configuration leaves closed; java.math is one.
    if (JAVA_SPEC_VER >= 16) {
      // setAccessible on the private BigInteger(int[]) constructor reports
      // java.lang.reflect.InaccessibleObjectException; the builder must count it as a
      // candidate miss instead of letting it escape the fallback chain.
      NoSuchMethodException thrown =
          assertThrows(
              NoSuchMethodException.class,
              () ->
                  DynConstructors.builder()
                      .hiddenImpl("java.math.BigInteger", int[].class)
                      .buildChecked());
      assertTrue(thrown.getMessage().contains("Cannot find constructor"));
      // a plain NoSuchMethodException would take the same branch if the JDK ever drops
      // this constructor; the suppressed problem pins the InaccessibleObjectException path
      assertTrue(
          Arrays.stream(thrown.getSuppressed())
              .anyMatch(
                  problem ->
                      "java.lang.reflect.InaccessibleObjectException"
                          .equals(problem.getClass().getName())));
    } else {
      // before strong encapsulation the same hidden lookup succeeds
      DynConstructors.Ctor<?> ctor =
          DynConstructors.builder().hiddenImpl("java.math.BigInteger", int[].class).buildChecked();
      assertEquals(BigInteger.class, ctor.getConstructedClass());
    }
  }

  @Test
  public void testHiddenImplPropagatesUnrelatedFailures() {
    // a RuntimeException that is not InaccessibleObjectException must still escape the
    // builder instead of being counted as a candidate miss
    // a builder with no class set dereferences the null base class inside the try block
    assertThrows(
        NullPointerException.class, () -> DynConstructors.builder().hiddenImpl((Class<?>[]) null));
  }
}
