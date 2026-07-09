# JDK 25 Subject.getSubject Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `KyuubiConnection.java` so Kerberos authentication works on JDK 25, where `Subject.getSubject()` and `AccessController.getContext()` throw `UnsupportedOperationException`.

**Architecture:** Add a `SubjectUtil.java` bridge class in the existing `auth` package that resolves `Subject.current()` (JDK 18+) via `MethodHandle` at class load time and falls back to `Subject.getSubject(AccessController.getContext())` on older JDKs. Replace the two broken callsites in `KyuubiConnection.java` with `SubjectUtil.current()`.

**Tech Stack:** Java, JUnit 5 (`junit-jupiter`), Maven (`build/mvn`)

---

## File Map

| Action | Path |
|--------|------|
| Create | `kyuubi-hive-jdbc/src/main/java/org/apache/kyuubi/jdbc/hive/auth/SubjectUtil.java` |
| Create | `kyuubi-hive-jdbc/src/test/java/org/apache/kyuubi/jdbc/hive/auth/SubjectUtilTest.java` |
| Modify | `kyuubi-hive-jdbc/src/main/java/org/apache/kyuubi/jdbc/hive/KyuubiConnection.java` |

---

### Task 1: Create `SubjectUtil` and verify it compiles and tests pass

**Files:**
- Create: `kyuubi-hive-jdbc/src/main/java/org/apache/kyuubi/jdbc/hive/auth/SubjectUtil.java`
- Create: `kyuubi-hive-jdbc/src/test/java/org/apache/kyuubi/jdbc/hive/auth/SubjectUtilTest.java`

- [ ] **Step 1: Write the failing test**

Create `kyuubi-hive-jdbc/src/test/java/org/apache/kyuubi/jdbc/hive/auth/SubjectUtilTest.java`:

```java
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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class SubjectUtilTest {

  @Test
  void current_returnsNullWhenNoSubjectAssociated() {
    // Outside of a Subject.doAs/callAs block there is no current subject.
    // This verifies SubjectUtil.current() neither throws nor returns a spurious value.
    assertNull(assertDoesNotThrow(SubjectUtil::current));
  }
}
```

- [ ] **Step 2: Run the test — expect compilation failure (class missing)**

```bash
build/mvn test -pl kyuubi-hive-jdbc -am -Dtest=SubjectUtilTest -DwildcardSuites=none 2>&1 | tail -20
```

Expected: Build error — `SubjectUtil` does not exist yet.

- [ ] **Step 3: Create `SubjectUtil.java`**

Create `kyuubi-hive-jdbc/src/main/java/org/apache/kyuubi/jdbc/hive/auth/SubjectUtil.java`:

```java
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
 * <p>JDK 18 introduced {@code Subject.current()} as a replacement for
 * {@code Subject.getSubject(AccessController.getContext())}. JDK 24+ made
 * {@code AccessController.getContext()} throw, and JDK 25 made
 * {@code Subject.getSubject()} throw. This class resolves the new API once at
 * class load time and falls back to the old API on JDK < 18.
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
   * <p>On JDK 18+ delegates to {@code Subject.current()}. On older JDKs delegates to
   * {@code Subject.getSubject(AccessController.getContext())}.
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
```

- [ ] **Step 4: Run the test — expect it to pass**

```bash
build/mvn test -pl kyuubi-hive-jdbc -am -Dtest=SubjectUtilTest -DwildcardSuites=none 2>&1 | tail -20
```

Expected output includes:
```
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

- [ ] **Step 5: Commit**

```bash
git add kyuubi-hive-jdbc/src/main/java/org/apache/kyuubi/jdbc/hive/auth/SubjectUtil.java \
        kyuubi-hive-jdbc/src/test/java/org/apache/kyuubi/jdbc/hive/auth/SubjectUtilTest.java
git commit -m "feat: add SubjectUtil to bridge Subject.getSubject() for JDK 25"
```

---

### Task 2: Replace broken callsites in `KyuubiConnection.java`

**Files:**
- Modify: `kyuubi-hive-jdbc/src/main/java/org/apache/kyuubi/jdbc/hive/KyuubiConnection.java`

**Context:** Two methods in `KyuubiConnection` call `Subject.getSubject(AccessController.getContext())`, which throws on JDK 25.

- [ ] **Step 1: Replace callsite in `isHadoopUserGroupInformationDoAs()` (line ~925)**

Find this block:
```java
  private boolean isHadoopUserGroupInformationDoAs() {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends Principal> HadoopUserClz =
          (Class<? extends Principal>) ClassUtils.getClass("org.apache.hadoop.security.User");
      Subject subject = Subject.getSubject(AccessController.getContext());
      return subject != null && !subject.getPrincipals(HadoopUserClz).isEmpty();
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
```

Replace with:
```java
  private boolean isHadoopUserGroupInformationDoAs() {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends Principal> HadoopUserClz =
          (Class<? extends Principal>) ClassUtils.getClass("org.apache.hadoop.security.User");
      Subject subject = SubjectUtil.current();
      return subject != null && !subject.getPrincipals(HadoopUserClz).isEmpty();
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
```

- [ ] **Step 2: Replace callsite in `createSubject()` (line ~1016)**

Find this block:
```java
  private Subject createSubject() {
    if (isKeytabAuthMode()) {
      String principal = sessConfMap.get(AUTH_KYUUBI_CLIENT_PRINCIPAL);
      String keytab = sessConfMap.get(AUTH_KYUUBI_CLIENT_KEYTAB);
      return KerberosAuthenticationManager.getKeytabAuthentication(principal, keytab).getSubject();
    } else if (isFromSubjectAuthMode()) {
      AccessControlContext context = AccessController.getContext();
      return Subject.getSubject(context);
    } else if (isTgtCacheAuthMode()) {
```

Replace with:
```java
  private Subject createSubject() {
    if (isKeytabAuthMode()) {
      String principal = sessConfMap.get(AUTH_KYUUBI_CLIENT_PRINCIPAL);
      String keytab = sessConfMap.get(AUTH_KYUUBI_CLIENT_KEYTAB);
      return KerberosAuthenticationManager.getKeytabAuthentication(principal, keytab).getSubject();
    } else if (isFromSubjectAuthMode()) {
      return SubjectUtil.current();
    } else if (isTgtCacheAuthMode()) {
```

- [ ] **Step 3: Compile the module to catch any errors**

```bash
build/mvn -Pfast clean package -DskipTests -pl kyuubi-hive-jdbc -am 2>&1 | tail -20
```

Expected:
```
BUILD SUCCESS
```

If you see `cannot find symbol: SubjectUtil`, verify the import at the top of `KyuubiConnection.java`. Add:
```java
import org.apache.kyuubi.jdbc.hive.auth.SubjectUtil;
```

(Check the existing `import org.apache.kyuubi.jdbc.hive.auth.*;` wildcard on line 62 — if present, no explicit import is needed.)

- [ ] **Step 4: Run the full hive-jdbc unit test suite**

```bash
build/mvn test -pl kyuubi-hive-jdbc -am -DwildcardSuites=none 2>&1 | tail -30
```

Expected: `BUILD SUCCESS` with no new test failures.

- [ ] **Step 5: Commit**

```bash
git add kyuubi-hive-jdbc/src/main/java/org/apache/kyuubi/jdbc/hive/KyuubiConnection.java
git commit -m "fix: replace Subject.getSubject() with SubjectUtil.current() for JDK 25 compat"
```

---

### Task 3: Format and open the PR

- [ ] **Step 1: Run the formatter**

```bash
dev/reformat 2>&1 | tail -10
```

If any files were reformatted, stage and amend the previous commit or add a new one:
```bash
git add -u
git commit -m "style: reformat after JDK 25 Subject fix"
```

- [ ] **Step 2: Open the PR**

```bash
gh pr create \
  --title "[KYUUBI #7549] Fix Subject.getSubject() for JDK 25 compatibility" \
  --body "$(cat <<'EOF'
## Why are the changes needed?

JDK 24 made `AccessController.getContext()` throw `UnsupportedOperationException` (JEP 486), and JDK 25 extended this to `Subject.getSubject()`. Both APIs are called in `KyuubiConnection.java` during Kerberos authentication setup, breaking JDBC connections on JDK 25 with:

```
java.lang.UnsupportedOperationException: getSubject is not supported
```

## How was this patch tested?

- Added `SubjectUtilTest` which verifies `SubjectUtil.current()` does not throw and returns `null` when no subject is associated with the current thread.
- Full `kyuubi-hive-jdbc` unit suite passes.
- Manual compile verification with `-Pfast` profile.

## Was this patch authored or co-authored using generative AI tooling?

Assisted-by: Claude:claude-sonnet-4-6
EOF
)"
```
