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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class DynFieldsTest {

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
    // AlwaysNull is an UnboundField<Void>: the compiler-generated bridge for its set(Object, Void)
    // override checkcasts the erased argument to Void, so null is the only writable value.
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
}
