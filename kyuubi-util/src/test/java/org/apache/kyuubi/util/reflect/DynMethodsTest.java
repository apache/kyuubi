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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class DynMethodsTest {

  // "1.8" -> 8, "9" -> 9, "17" -> 17, etc.
  private static final int JAVA_SPEC_VER =
      Math.max(
          8, Integer.parseInt(System.getProperty("java.specification.version").split("\\.")[0]));

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
  public void testHiddenImplHandlesStronglyEncapsulatedMethods() throws Exception {
    // Pin the probe: if a future JDK drops this member the lookup would miss for an unrelated
    // reason and the assertions below would pass while covering nothing.
    assertNotNull(BigDecimal.class.getDeclaredMethod("checkScale", long.class));

    // The test JVM opens java.base/java.lang to the unnamed module, so the encapsulated
    // path needs a package the surefire configuration leaves closed; java.math is one.
    if (JAVA_SPEC_VER >= 16) {
      // setAccessible on BigDecimal.checkScale reports InaccessibleObjectException; the
      // builder must count it as a candidate miss instead of letting it escape the fallback
      // chain.
      RuntimeException thrown =
          assertThrows(
              RuntimeException.class,
              () ->
                  DynMethods.builder("checkScale")
                      .hiddenImpl("java.math.BigDecimal", long.class)
                      .build());
      assertEquals(RuntimeException.class, thrown.getClass());
      assertTrue(thrown.getMessage().contains("Cannot find method"));
      // a plain NoSuchMethodException would take the same branch if the JDK ever drops this
      // method; the suppressed problem pins the InaccessibleObjectException path
      assertTrue(
          Arrays.stream(thrown.getSuppressed())
              .anyMatch(
                  problem ->
                      "java.lang.reflect.InaccessibleObjectException"
                          .equals(problem.getClass().getName())),
          () -> "unexpected suppressed problems: " + Arrays.toString(thrown.getSuppressed()));
    } else {
      // before strong encapsulation the same hidden lookup succeeds
      assertNotNull(
          DynMethods.builder("checkScale").hiddenImpl("java.math.BigDecimal", long.class).build());
    }
  }

  @Test
  public void testHiddenImplPropagatesUnrelatedFailures() {
    // a RuntimeException that is not InaccessibleObjectException must still escape the
    // builder instead of being counted as a candidate miss
    assertThrows(
        NullPointerException.class,
        () -> DynMethods.builder("checkScale").hiddenImpl((Class<?>) null, "checkScale"));
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

  @Test
  public void testBuildFailureReportsCandidateProblems() {
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                DynMethods.builder("noSuchMethod")
                    .impl(ReflectionTarget.class, String.class)
                    .build());
    assertEquals("Cannot find method: noSuchMethod", headerOf(thrown.getMessage()));
    assertEquals(
        Arrays.asList(ReflectionTarget.class.getName() + "#noSuchMethod(java.lang.String)"),
        candidatesIn(thrown.getMessage()));
    assertEquals(1, thrown.getSuppressed().length);
    assertTrue(thrown.getSuppressed()[0] instanceof NoSuchMethodException);
    // the recorded throwable has to be the one the lookup raised: its stack is what says which
    // candidate in the caller's chain missed, and a substitute would lose that
    assertRaisedByTheLookup(thrown.getSuppressed()[0]);
  }

  @Test
  public void testBuildCheckedFailureReportsCandidateProblems() {
    NoSuchMethodException thrown =
        assertThrows(
            NoSuchMethodException.class,
            () ->
                DynMethods.builder("noSuchMethod")
                    .impl("a.b.MissingClass")
                    .impl("c.d.AlsoMissing")
                    .buildChecked());
    assertEquals(
        "Cannot find method: noSuchMethod\n"
            + "\tMissing a.b.MissingClass#noSuchMethod() "
            + "[java.lang.ClassNotFoundException: a.b.MissingClass]\n"
            + "\tMissing c.d.AlsoMissing#noSuchMethod() "
            + "[java.lang.ClassNotFoundException: c.d.AlsoMissing]",
        thrown.getMessage());
    // a ClassNotFoundException reports the name it was given, so the suppressed order is readable
    // straight off the messages
    assertEquals(
        Arrays.asList("a.b.MissingClass", "c.d.AlsoMissing"),
        Arrays.stream(thrown.getSuppressed())
            .map(Throwable::getMessage)
            .collect(Collectors.toList()));
  }

  @Test
  public void testImplTreatsANullArgumentClassAsAnOrdinaryMiss() {
    // DynClasses.orNull() hands back null for a class the running dependency does not provide, and
    // callers pass that straight into impl, so the chain has to fall through to the next candidate
    DynMethods.UnboundMethod method =
        DynMethods.builder("echo")
            .impl(ReflectionTarget.class, (Class<?>) null)
            .impl(ReflectionTarget.class, String.class)
            .build();
    assertEquals("hi", method.invoke(new ReflectionTarget(), "hi"));
  }

  @Test
  public void testBuildFailureNamesANullArgumentClass() {
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                DynMethods.builder("noSuchMethod")
                    .hiddenImpl(ReflectionTarget.class, (Class<?>) null)
                    .build());
    assertEquals(
        Arrays.asList("declared " + ReflectionTarget.class.getName() + "#noSuchMethod(null)"),
        candidatesIn(thrown.getMessage()));
    // getMethod reads a null array as no arguments at all, so the candidate name has to as well
    RuntimeException noArgs =
        assertThrows(
            RuntimeException.class,
            () ->
                DynMethods.builder("noSuchMethod")
                    .impl(ReflectionTarget.class, (Class<?>[]) null)
                    .build());
    assertEquals(
        Arrays.asList(ReflectionTarget.class.getName() + "#noSuchMethod()"),
        candidatesIn(noArgs.getMessage()));
    assertTrue(thrown.getSuppressed()[0] instanceof NoSuchMethodException);
    assertRaisedByTheLookup(thrown.getSuppressed()[0]);
  }

  @Test
  public void testBuildFailureLeavesNoTrailingLineWhenNothingWasTried() {
    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> DynMethods.builder("neverLookedUp").build());
    assertEquals("Cannot find method: neverLookedUp", thrown.getMessage());
    assertEquals(0, thrown.getSuppressed().length);
  }

  @Test
  public void testBuildFailureReportsHiddenImplClassProblems() {
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                DynMethods.builder("noSuchMethod")
                    .hiddenImpl("a.b.MissingClass", "lookedUpName", String.class)
                    .build());
    assertEquals(
        Arrays.asList("declared a.b.MissingClass#lookedUpName(java.lang.String)"),
        candidatesIn(thrown.getMessage()));
    assertEquals(1, thrown.getSuppressed().length);
    assertTrue(thrown.getSuppressed()[0] instanceof ClassNotFoundException);
    // the recorded throwable has to be the one the lookup raised, since its stack is what says
    // which candidate in the caller's chain missed
    assertEquals("a.b.MissingClass", thrown.getSuppressed()[0].getMessage());
  }

  @Test
  public void testBuildFailureReportsCtorImplProblems() {
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                DynMethods.builder("create")
                    .ctorImpl(ReflectionTarget.class, String.class, int.class)
                    .build());
    assertEquals("Cannot find method: create", headerOf(thrown.getMessage()));
    assertEquals(
        Arrays.asList(ReflectionTarget.class.getName() + "#<init>(java.lang.String,int)"),
        candidatesIn(thrown.getMessage()));
    assertTrue(
        thrown.getMessage().contains("constructor for class " + ReflectionTarget.class.getName()),
        thrown::getMessage);
    assertEquals(1, thrown.getSuppressed().length);
    assertTrue(thrown.getSuppressed()[0] instanceof NoSuchMethodException);
    assertEquals(1, thrown.getSuppressed()[0].getSuppressed().length);
  }

  @Test
  public void testBuildFailureListsProblemsInAttemptOrder() {
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                DynMethods.builder("noSuchMethod")
                    .impl("a.b.MissingClass")
                    .impl(ReflectionTarget.class, String.class)
                    .hiddenImpl(ReflectionTarget.class, "noSuchMethod")
                    .build());
    String target = ReflectionTarget.class.getName();
    List<String> candidates =
        Arrays.asList(
            "a.b.MissingClass#noSuchMethod()",
            target + "#noSuchMethod(java.lang.String)",
            "declared " + target + "#noSuchMethod()");
    assertEquals(candidates, candidatesIn(thrown.getMessage()));
    // Every entry has to carry the failure of the candidate it names, and addSuppressed is fed the
    // same values in the same order, so rebuilding the entries from the suppressed array in order
    // has to reproduce the message.
    Throwable[] suppressed = thrown.getSuppressed();
    assertEquals(candidates.size(), suppressed.length);
    List<String> rebuilt = new ArrayList<>();
    for (int i = 0; i < suppressed.length; i++) {
      int index = i;
      assertTrue(
          suppressed[index]
              .getMessage()
              .contains(candidates.get(index).replace("declared ", "").split("#")[0]),
          () -> "a suppressed failure does not describe its candidate: " + suppressed[index]);
      rebuilt.add(
          candidates.get(i)
              + " ["
              + suppressed[i].getClass().getName()
              + ": "
              + suppressed[i].getMessage()
              + "]");
    }
    assertEquals(rebuilt, entriesIn(thrown.getMessage()));
  }

  @Test
  public void testBuildFailureNamesTheLookedUpMethodNotTheBuilder() {
    String target = ReflectionTarget.class.getName();
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                DynMethods.builder("builderName")
                    .impl(ReflectionTarget.class, "lookedUpName", String.class, int.class)
                    .impl(target, "alsoLookedUp", String.class, int.class)
                    .hiddenImpl(ReflectionTarget.class, "hiddenLookedUp", String.class, int.class)
                    .hiddenImpl(target, "hiddenByName", String.class, int.class)
                    .build());
    assertEquals("Cannot find method: builderName", headerOf(thrown.getMessage()));
    assertEquals(
        Arrays.asList(
            target + "#lookedUpName(java.lang.String,int)",
            target + "#alsoLookedUp(java.lang.String,int)",
            "declared " + target + "#hiddenLookedUp(java.lang.String,int)",
            "declared " + target + "#hiddenByName(java.lang.String,int)"),
        candidatesIn(thrown.getMessage()));
  }

  @Test
  public void testDeclaredAndPublicLookupsOfOneSignatureBothReport() {
    // the shape ReflectUtils.invokeAs uses. getDeclaredMethod searches one class at every
    // visibility and getMethod searches the hierarchy at public only, so neither miss implies the
    // other and reporting one of the two would leave the reader guessing which ran.
    String target = ReflectionTarget.class.getName();
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                DynMethods.builder("noSuchMethod")
                    .hiddenImpl(ReflectionTarget.class, String.class)
                    .impl(ReflectionTarget.class, String.class)
                    .build());
    assertEquals(
        Arrays.asList(
            "declared " + target + "#noSuchMethod(java.lang.String)",
            target + "#noSuchMethod(java.lang.String)"),
        candidatesIn(thrown.getMessage()));
    assertEquals(2, thrown.getSuppressed().length);
  }

  @Test
  public void testOneCandidateNameCanCoverTwoUnrelatedReasons() throws Exception {
    // Both lookups name the same candidate, and they miss for unrelated reasons: the configured
    // loader cannot see the class at all, then the class that is on the classpath turns out to lack
    // the method. Keeping one of the two would report the wrong reason for the failure.
    String target = ReflectionTarget.class.getName();
    RuntimeException thrown;
    try (URLClassLoader blinded = new URLClassLoader(new URL[0], null)) {
      thrown =
          assertThrows(
              RuntimeException.class,
              () ->
                  DynMethods.builder("noSuchMethod")
                      .loader(blinded)
                      .impl(target, "noSuchMethod", String.class)
                      .impl(ReflectionTarget.class, "noSuchMethod", String.class)
                      .build());
    }
    String candidate = target + "#noSuchMethod(java.lang.String)";
    assertEquals(Arrays.asList(candidate, candidate), candidatesIn(thrown.getMessage()));
    assertEquals(
        Arrays.asList(ClassNotFoundException.class, NoSuchMethodException.class),
        Arrays.stream(thrown.getSuppressed())
            .map(Throwable::getClass)
            .collect(Collectors.toList()));
  }

  @Test
  public void testTheSameLookupWrittenTwiceReportsTwice() {
    // one entry per lookup, not per distinct name: a repeat is still a lookup that ran and missed
    String target = ReflectionTarget.class.getName();
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                DynMethods.builder("noSuchMethod")
                    .impl(ReflectionTarget.class, String.class)
                    .impl(ReflectionTarget.class, String.class)
                    .build());
    String candidate = target + "#noSuchMethod(java.lang.String)";
    assertEquals(Arrays.asList(candidate, candidate), candidatesIn(thrown.getMessage()));
    assertEquals(2, thrown.getSuppressed().length);
  }

  @Test
  public void testCandidateWithoutADetailMessageIsStillReported() {
    // a loader may raise ClassNotFoundException with no message of its own
    ClassLoader silent =
        new ClassLoader(null) {
          @Override
          public Class<?> loadClass(String name) throws ClassNotFoundException {
            throw new ClassNotFoundException();
          }
        };
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                DynMethods.builder("noSuchMethod").loader(silent).impl("a.b.MissingClass").build());
    assertEquals(
        "Cannot find method: noSuchMethod\n"
            + "\tMissing a.b.MissingClass#noSuchMethod() "
            + "[java.lang.ClassNotFoundException: null]",
        thrown.getMessage());
  }

  @Test
  public void testBuildFailureReportsCtorImplClassLoadProblems() {
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> DynMethods.builder("create").ctorImpl("a.b.MissingClass").build());
    assertEquals("Cannot find method: create", headerOf(thrown.getMessage()));
    assertEquals(Arrays.asList("a.b.MissingClass#<init>()"), candidatesIn(thrown.getMessage()));
    // the delegated lookup's own detail is why this entry is worth anything, and it has to sit a
    // level deeper so it does not read as a candidate of this builder
    assertTrue(
        thrown
            .getMessage()
            .contains("\n\t\tMissing a.b.MissingClass [java.lang.ClassNotFoundException: "),
        thrown::getMessage);
    assertEquals(1, thrown.getSuppressed().length);
    assertTrue(thrown.getSuppressed()[0] instanceof NoSuchMethodException);
    assertEquals(1, thrown.getSuppressed()[0].getSuppressed().length);
  }

  @Test
  public void testCtorImplRecordsEachConstructorSignatureSeparately() {
    String target = ReflectionTarget.class.getName();
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                DynMethods.builder("create")
                    .ctorImpl(target, int.class)
                    .ctorImpl(target, long.class)
                    .build());
    assertEquals(
        Arrays.asList(target + "#<init>(int)", target + "#<init>(long)"),
        candidatesIn(thrown.getMessage()));
    assertEquals(2, thrown.getSuppressed().length);
  }

  @Test
  public void testAnUnloadableClassIsNamedWithTheLookedUpMethod() throws Exception {
    // the loader raises a sentinel, so the entry can be checked to carry the very throwable the
    // lookup raised, and the method name asked for rather than the builder's own
    ClassNotFoundException sentinel = new ClassNotFoundException("sentinel");
    ClassLoader raising =
        new ClassLoader(null) {
          @Override
          public Class<?> loadClass(String name) throws ClassNotFoundException {
            throw sentinel;
          }
        };
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                DynMethods.builder("builderName")
                    .loader(raising)
                    .impl("a.b.MissingClass", "lookedUpName", String.class)
                    .build());
    assertEquals(
        Arrays.asList("a.b.MissingClass#lookedUpName(java.lang.String)"),
        candidatesIn(thrown.getMessage()));
    assertSame(sentinel, thrown.getSuppressed()[0]);
  }

  @Test
  public void testANestedMessageIsIndentedLineByLine() throws Exception {
    // the delegated lookup's cause can span lines of its own, and indenting has to move every one
    // of them, not only the ones that already start with a tab
    ClassLoader multiline =
        new ClassLoader(null) {
          @Override
          public Class<?> loadClass(String name) throws ClassNotFoundException {
            throw new ClassNotFoundException("first line\nsecond line");
          }
        };
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                DynMethods.builder("create")
                    .loader(multiline)
                    .ctorImpl("a.b.MissingClass", String.class)
                    .build());
    assertTrue(thrown.getMessage().contains("first line\n\tsecond line"), thrown::getMessage);
  }

  @Test
  public void testCtorImplTreatsANullArgumentClassAsAnOrdinaryMiss() {
    // the guard that makes this a miss rather than a throw lives in DynConstructors, which the
    // constructor lookups delegate to
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                DynMethods.builder("create")
                    .ctorImpl(ReflectionTarget.class, (Class<?>) null)
                    .build());
    assertEquals(
        Arrays.asList(ReflectionTarget.class.getName() + "#<init>(null)"),
        candidatesIn(thrown.getMessage()));
  }

  @Test
  public void testOrNoopStillBuildsWithMissesRecorded() {
    // RowSet.getBinaryFormatter is this shape, and on Spark 3.5 the impl always misses
    DynMethods.UnboundMethod noop =
        DynMethods.builder("noSuchMethod")
            .impl(ReflectionTarget.class, String.class)
            .orNoop()
            .build();

    assertTrue(noop.isNoop());
  }

  /**
   * Fails if the throwable was built by this package rather than raised by the reflective call, in
   * which case the stack that says which candidate missed is gone.
   */
  private static void assertRaisedByTheLookup(Throwable recorded) {
    StackTraceElement[] stack = recorded.getStackTrace();
    assertTrue(stack.length > 0, () -> "no stack on " + recorded);
    assertFalse(
        stack[0].getClassName().startsWith("org.apache.kyuubi"),
        () -> "substituted for the lookup's own failure: " + stack[0]);
  }

  private static String headerOf(String message) {
    return message.split("\n", -1)[0];
  }

  /** Returns each of this builder's entries, in the order it appears. */
  private static List<String> entriesIn(String message) {
    List<String> entries = new ArrayList<>();
    for (String entry : message.split("\n\tMissing ")) {
      if (entry.contains(" [")) {
        entries.add(entry);
      }
    }
    return entries;
  }

  /** Returns the candidate name of each of this builder's entries, in the order it appears. */
  private static List<String> candidatesIn(String message) {
    List<String> candidates = new ArrayList<>();
    for (String entry : entriesIn(message)) {
      candidates.add(entry.substring(0, entry.indexOf(" [")));
    }
    return candidates;
  }
}
