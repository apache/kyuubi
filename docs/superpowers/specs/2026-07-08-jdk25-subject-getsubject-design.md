# JDK 25 Compatibility: Fix `Subject.getSubject()` in JDBC Driver

**Date:** 2026-07-08  
**Issue:** [KYUUBI #7549](https://github.com/apache/kyuubi/issues/7549)  
**Scope:** `kyuubi-hive-jdbc` module only

---

## Problem

JDK 24 made `AccessController.getContext()` throw `UnsupportedOperationException` (JEP 486), and JDK 25 extended this to `Subject.getSubject()`. Kyuubi's JDBC driver calls both APIs in `KyuubiConnection.java`, causing authentication to fail at runtime on JDK 25:

```
java.lang.UnsupportedOperationException: getSubject is not supported
    at java.base/javax.security.auth.Subject.getSubject(...)
```

Both APIs still compile on JDK 25 — they only fail at runtime. The JDK 18+ replacements are `Subject.current()` (for `getSubject`) and `Subject.callAs()` (for `doAs`). Kyuubi must support JDK 8 through 25, so compile-time use of `Subject.current()` is not an option.

---

## Out of Scope

- `Subject.doAs()` calls in `HttpAuthUtils`, `TSubjectTransport`, `SpnegoAuthHeaderGenerator`, and `KerberosAuthenticationHandler` — deprecated but not broken in JDK 25; deferred to a follow-up.
- Adding JDK 25 to the CI matrix — separate concern.

---

## Design

### New file: `SubjectUtil.java`

**Location:** `kyuubi-hive-jdbc/src/main/java/org/apache/kyuubi/jdbc/hive/auth/SubjectUtil.java`

Uses `MethodHandles` to resolve `Subject.current()` once at class load time. If the method exists (JDK 18+), the handle is stored and used. If not (`NoSuchMethodException` on JDK < 18), the handle stays `null` and the fallback path runs instead.

```
static init:
  try MethodHandles.lookup().findStatic(Subject.class, "current", ...)
    → store handle (JDK 18+)
  catch NoSuchMethodException
    → handle = null (JDK < 18)

current():
  if handle != null → invoke handle
  else              → Subject.getSubject(AccessController.getContext())
```

The fallback branch compiles and links on all JDK versions. On JDK 25 it is never reached because the handle is populated.

### Changes to `KyuubiConnection.java`

Two callsites replaced with `SubjectUtil.current()`:

|                    Method                     |                              Before                               |          After          |
|-----------------------------------------------|-------------------------------------------------------------------|-------------------------|
| `isHadoopUserGroupInformationDoAs()` line 925 | `Subject.getSubject(AccessController.getContext())`               | `SubjectUtil.current()` |
| `createSubject()` lines 1022–1023             | `AccessControlContext context = ...; Subject.getSubject(context)` | `SubjectUtil.current()` |

No import changes needed — `KyuubiConnection.java` uses a wildcard `import java.security.*;`; removing the two callsites leaves the wildcard intact with no dangling references.

### No new tests

The `SubjectUtil.current()` method has no branching business logic — it is a one-line delegation to whichever API is available at runtime. Existing Kerberos integration tests in `kyuubi-hive-jdbc` cover both callsites end-to-end. A JDK 25 CI job would serve as the regression gate.

---

## Files Changed

|                                        File                                        |                    Change                    |
|------------------------------------------------------------------------------------|----------------------------------------------|
| `kyuubi-hive-jdbc/src/main/java/org/apache/kyuubi/jdbc/hive/auth/SubjectUtil.java` | New                                          |
| `kyuubi-hive-jdbc/src/main/java/org/apache/kyuubi/jdbc/hive/KyuubiConnection.java` | 2 callsites replaced, unused imports removed |

