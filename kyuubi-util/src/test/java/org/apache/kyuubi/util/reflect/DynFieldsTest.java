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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import org.junit.jupiter.api.Test;

public class DynFieldsTest {

  // "1.8" -> 8, "9" -> 9, "17" -> 17, etc.
  private static final int JAVA_SPEC_VER =
      Math.max(
          8, Integer.parseInt(System.getProperty("java.specification.version").split("\\.")[0]));

  private static class ReflectionTarget {
    public static String staticField = "static-value";

    public String instanceField = "instance-value";
  }

  private static class OtherTarget {}

  @Test
  public void testAlwaysNullBindsToAnyTarget() {
    DynFields.BoundField<String> bound =
        DynFields.builder()
            .impl(ReflectionTarget.class, "noSuchField")
            .defaultAlwaysNull()
            .build(new OtherTarget());

    assertNull(bound.get());
    // AlwaysNull is an UnboundField<Void>, so the bridge generated for its set(Object, Void)
    // override casts the erased argument to Void: a side effect of the signature, not a contract.
    assertThrows(ClassCastException.class, () -> bound.set("any-value"));
    bound.set(null);
    assertNull(bound.get());
  }

  @Test
  public void testAlwaysNullBindsThroughEveryEntryPoint() throws NoSuchFieldException {
    DynFields.Builder builder =
        DynFields.builder().impl(ReflectionTarget.class, "noSuchField").defaultAlwaysNull();

    DynFields.UnboundField<String> alwaysNull = builder.build();
    assertTrue(alwaysNull.isAlwaysNull());
    assertTrue(alwaysNull.isStatic());

    DynFields.BoundField<String> viaBind = alwaysNull.bind(new OtherTarget());
    assertNull(viaBind.get());

    DynFields.BoundField<String> viaBuild = builder.build(new OtherTarget());
    assertNull(viaBuild.get());

    DynFields.BoundField<String> viaBuildChecked = builder.buildChecked(new ReflectionTarget());
    assertNull(viaBuildChecked.get());
  }

  @Test
  public void testAlwaysNullFromMissingClassNameBindsToAnyTarget() {
    DynFields.BoundField<String> bound =
        DynFields.builder()
            .impl("a.b.MissingClass", "noSuchField")
            .defaultAlwaysNull()
            .build(new ReflectionTarget());

    assertNull(bound.get());
  }

  @Test
  public void testAlwaysNullAsStaticReturnsNull() throws NoSuchFieldException {
    DynFields.StaticField<String> viaBuildStatic =
        DynFields.builder()
            .impl(ReflectionTarget.class, "noSuchField")
            .defaultAlwaysNull()
            .buildStatic();
    assertNull(viaBuildStatic.get());

    DynFields.StaticField<String> viaBuildStaticChecked =
        DynFields.builder()
            .impl(ReflectionTarget.class, "noSuchField")
            .defaultAlwaysNull()
            .buildStaticChecked();
    assertNull(viaBuildStaticChecked.get());
  }

  @Test
  public void testAlwaysNullToStringReportsSentinelName() {
    DynFields.UnboundField<String> alwaysNull =
        DynFields.builder().impl(ReflectionTarget.class, "noSuchField").defaultAlwaysNull().build();

    assertEquals("Field(AlwaysNull)", alwaysNull.toString());
  }

  @Test
  public void testRealFieldIsNotAlwaysNull() {
    DynFields.UnboundField<String> realField =
        DynFields.builder().impl(ReflectionTarget.class, "instanceField").build();

    assertFalse(realField.isAlwaysNull());
    assertFalse(realField.isStatic());
  }

  @Test
  public void testBuildStaticReadsRealStaticField() {
    DynFields.StaticField<String> staticField =
        DynFields.builder().impl(ReflectionTarget.class, "staticField").buildStatic();

    assertEquals("static-value", staticField.get());
  }

  @Test
  public void testBuildWithoutAlwaysNullFallbackThrows() {
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> DynFields.builder().impl(ReflectionTarget.class, "noSuchField").build());

    assertTrue(thrown.getMessage().contains("Cannot find field from candidates"));
  }

  @Test
  public void testBindRejectsStaticField() {
    DynFields.UnboundField<String> staticField =
        DynFields.builder().impl(ReflectionTarget.class, "staticField").build();

    IllegalStateException thrown =
        assertThrows(IllegalStateException.class, () -> staticField.bind(new ReflectionTarget()));
    assertEquals("Cannot bind static field staticField", thrown.getMessage());
  }

  @Test
  public void testBindRejectsIncompatibleTarget() {
    DynFields.UnboundField<String> instanceField =
        DynFields.builder().impl(ReflectionTarget.class, "instanceField").build();

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> instanceField.bind(new OtherTarget()));
    assertTrue(thrown.getMessage().contains("Cannot bind field instanceField to instance of"));
  }

  @Test
  public void testBoundInstanceFieldGetAndSet() {
    ReflectionTarget target = new ReflectionTarget();
    DynFields.BoundField<String> bound =
        DynFields.builder().impl(ReflectionTarget.class, "instanceField").build(target);

    assertEquals("instance-value", bound.get());
    bound.set("new-value");
    assertEquals("new-value", bound.get());
    assertEquals("new-value", target.instanceField);
  }

  @Test
  public void testHiddenImplTreatsStronglyEncapsulatedFieldsAsMisses() throws Exception {
    // Pin the probe: if a future JDK drops this member the lookup would miss for an unrelated
    // reason and the assertions below would pass while covering nothing.
    assertNotNull(BigInteger.class.getDeclaredField("signum"));

    // The test JVM opens java.base/java.lang to the unnamed module, so the encapsulated
    // path needs a package the surefire configuration leaves closed; java.math is one.
    if (JAVA_SPEC_VER >= 16) {
      // setAccessible on BigInteger.signum reports InaccessibleObjectException; the builder
      // must count it as a candidate miss instead of letting it escape the fallback chain.
      RuntimeException thrown =
          assertThrows(
              RuntimeException.class,
              () -> DynFields.builder().hiddenImpl("java.math.BigInteger", "signum").build());
      assertEquals(RuntimeException.class, thrown.getClass());
      assertTrue(thrown.getMessage().contains("Cannot find field"));
      // the candidate carries the exception, so these pin the encapsulation path itself rather
      // than any candidate miss, and keep the module and package to open in the message
      assertTrue(thrown.getMessage().contains("InaccessibleObjectException"));
      assertTrue(thrown.getMessage().contains("opens java.math"));
    } else {
      // before strong encapsulation the same hidden lookup succeeds
      DynFields.UnboundField<?> signum =
          DynFields.builder().hiddenImpl("java.math.BigInteger", "signum").build();
      assertNotNull(signum.bind(BigInteger.ONE).get());
    }
  }

  @Test
  public void testHiddenImplPropagatesUnrelatedFailures() {
    // a RuntimeException that is not InaccessibleObjectException must still escape the
    // builder instead of being counted as a candidate miss. The null goes to fieldName
    // because hiddenImpl guards targetClass == null at the top; getDeclaredField(null) NPEs
    // on every supported JDK (name.intern() on 8/11, requireNonNull at entry on 17+).
    assertThrows(
        NullPointerException.class,
        () -> DynFields.builder().hiddenImpl(ReflectionTarget.class, null));
  }
}
