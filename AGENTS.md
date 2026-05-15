<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Apache Kyuubi — Agent Instructions

A guide for AI coding agents (Claude Code, Codex, Cursor, Copilot, Gemini,
etc.) contributing to Apache Kyuubi. Pairs with `CONTRIBUTING.md` and the
[Contributor Guide](https://kyuubi.readthedocs.io/en/master/contributing/code/index.html);
this file is terser, command-oriented, and surfaces conventions that
recur in code review.

## Pre-flight Checks

Before the first edit or test in a session:

1. `git remote -v` — confirm `upstream` (or equivalent) points to
   `apache/kyuubi`. If only a personal fork is configured, ask the user
   to add one.
2. If the latest commit on `<upstream>/master` is more than a day old
   (`git log -1 --format="%ci" <upstream>/master`), run
   `git fetch <upstream> master`.
3. `git status` must be clean; ask the user to stash uncommitted work
   before proceeding.
4. Pick a working branch:
   - **Existing PR**: resolve via
     `gh api repos/apache/kyuubi/pulls/<num> --jq '.head.ref'` and check
     out that branch (or fetch it).
   - **New work**: branch from `<upstream>/master` (e.g.
     `git switch -c kyuubi-NNNN-short-slug <upstream>/master`).
5. Ensure `git config user.email` matches an email linked to the GitHub
   account that will open the PR — otherwise the contribution is not
   counted on GitHub. Reviewers will block on this.

## Architecture

Kyuubi is a multi-tenant gateway that fronts pluggable SQL engines
(Spark, Flink, Hive, Trino, JDBC). The architecture is non-negotiable
in code review.

### Module Layout

- `kyuubi-server/` — the gateway process. Engine-agnostic. **Must not
  depend on engine libraries** (Spark, Flink, Hive, Trino classes).
- `kyuubi-common/` — shared utilities, Thrift IDL, base classes.
- `kyuubi-ha/`, `kyuubi-events/`, `kyuubi-metrics/`,
  `kyuubi-rest-client/`, `kyuubi-zookeeper/` — focused libraries used by
  the server and engines.
- `kyuubi-ctl/` — admin CLI.
- `kyuubi-hive-jdbc/`, `kyuubi-hive-beeline/` — JDBC client and shell.
- `externals/kyuubi-spark-sql-engine/`,
  `externals/kyuubi-flink-sql-engine/`,
  `externals/kyuubi-hive-sql-engine/`,
  `externals/kyuubi-trino-engine/`,
  `externals/kyuubi-jdbc-engine/`,
  `externals/kyuubi-chat-engine/`,
  `externals/kyuubi-data-agent-engine/` — engine processes.
  **Engine modules must not depend on each other.** Each engine
  process runs detached from the server and from other engine
  processes.
- `extensions/{server,spark,flink}/` — opt-in plug-ins.
- `integration-tests/` — cross-module integration suites.
- `kyuubi-assembly/` — produces the binary distribution.

### Hard Boundaries

- **Server must not import engine code.** `kyuubi-server` cannot have a
  compile-time dependency on Spark/Flink/Hive/Trino. Detect indirect
  pulls with `mvn dependency:tree`. If you genuinely need engine logic
  server-side, the answer is almost always "you don't — move it to the
  engine and call over Thrift."
- **Engines must not depend on sibling engines.** No
  `kyuubi-flink-sql-engine` → `kyuubi-spark-sql-engine` dependencies.
- **Server-only configs go in `serverOnlyConfEntries`** in
  `KyuubiConf`. Otherwise they leak to the engine side.
- **Public APIs are abstract over the cluster manager.** Use
  `killApplication`, not `closeYarnJob`. Methods, classes, and config
  keys must work whether the engine is on YARN, Kubernetes, or a future
  CM.
- **No SQL syntax that Spark itself doesn't support.** Extensions must
  not diverge into a Kyuubi-specific SQL dialect.
- **Don't break the Thrift wire protocol.** Kyuubi speaks Hive's
  TCLIService over Thrift via the `kyuubi-relocated-thrift`
  dependency; the canonical IDL lives upstream in Hive. The only
  Kyuubi-side schema delta is `python/scripts/thrift-patches/`. Wire
  changes are extremely high-risk and need explicit maintainer
  sign-off; never remove or renumber fields.

### High-Sensitivity Areas

Treat these as load-bearing; check with reviewers before changing:

- `python/scripts/thrift-patches/` — patches against the upstream Hive
  TCLIService schema. Anything touching wire compatibility.
- `kyuubi-common/.../session/SessionManager`,
  `kyuubi-common/.../operation/OperationManager` — session and
  operation lifecycle. Concurrency and shutdown semantics are subtle.
- `kyuubi-server/.../engine/KubernetesApplicationOperation` — cluster
  manager integration. Tests must not assume a real cluster.
- `kyuubi-common/.../config/KyuubiConf` — configuration registry.
  Changes require `settings.md` regeneration (see §Configuration).
- REST endpoints under `kyuubi-server/.../api/v1/` — public surface;
  add auth checks before exposing.

## Build and Test

Maven only (no SBT). Use the bundled wrapper to ensure the project's
Maven version: `build/mvn` (it auto-installs the right Maven into
`build/apache-maven-*`).

### Fast iteration

```
build/mvn -Pfast clean package -DskipTests
```

The `fast` profile skips tests, style checks, scaladoc, enforcer rules,
and bundled-engine downloads. Use it when you only need to verify
compilation.

### Single-module build

```
build/mvn clean package -pl kyuubi-common -DskipTests
build/mvn clean package -pl kyuubi-common,kyuubi-ha -DskipTests
```

### Engine profile matrix

Kyuubi compiles against multiple Spark/Flink/Hive/Trino versions via
Maven profiles. The default is the most recent supported version of
each engine.

| Profile | Notes |
|---|---|
| `-Pspark-3.5` | default Spark |
| `-Pspark-3.4`, `-Pspark-3.3`, `-Pspark-4.0`, `-Pspark-4.1` | other supported Sparks |
| `-Pscala-2.13` | Scala 2.13 build (default is 2.12) |
| `-Pflink-provided`, `-Pspark-provided`, `-Phive-provided` | exclude bundled engine downloads |
| `-Pmirror-cdn` | use the Apache mirror CDN for engine archives |
| `-Pfast` | skip tests/style/docs/enforcer/downloads |

When adding code that varies across engine versions, gate it with the
matching profile, not a runtime version check.

### Running tests

All tests:

```
build/mvn clean install
```

Single module:

```
build/mvn clean install -pl kyuubi-common
```

Single Scala suite:

```
build/mvn test -pl kyuubi-server -Dtest=none \
    -DwildcardSuites=org.apache.kyuubi.server.api.v1.SessionsResourceSuite
```

Single Java test:

```
build/mvn test -pl kyuubi-hive-jdbc -Dtest=KyuubiStatementTest -DwildcardSuites=none
```

Tests for `kyuubi-spark-sql-engine` etc. require building a binary
distribution first (`build/dist`).

### Style and formatting

```
dev/reformat
```

Runs Spotless (google-java-format + Scalafmt) and Python `black` if
available. Run before every commit; format violations are a routine
reviewer complaint that costs a round trip.

### Dependency list

After adding/removing a runtime dependency:

```
build/dependency.sh                # detect drift
build/dependency.sh --replace      # update dev/dependencyList
```

CI fails if `dev/dependencyList` is out of sync.

### Regenerating `settings.md`

After adding or modifying any `ConfigEntry` in `KyuubiConf`:

```
dev/gen/gen_all_config_docs.sh
```

This runs the scoped `AllKyuubiConfiguration` test under
`kyuubi-server` with `KYUUBI_UPDATE=1` and regenerates
`docs/configuration/settings.md`. Commit the diff.

## Coding Conventions

### Languages

- New business logic: **Java by default.** Use Scala only for code that
  sits in Scala-heavy framework layers (e.g. Spark engine internals,
  Scala test suites). Mixing Java callers with Scala-only libraries
  (Option/Either etc.) creates friction at module boundaries.
- Scala code follows the
  [Databricks Scala Coding Style Guide](https://github.com/databricks/scala-style-guide).
- Java code follows the
  [Google Java Style](https://google.github.io/styleguide/javaguide.html).
- Formatting is enforced by Spotless; `dev/reformat` is authoritative.

### Scala specifics

- Use `Option[T]`, never `null`, for nullable values.
- Prefer pattern-match guards (`case X(...) if cond =>`) over nested
  `if`/`match`.
- Add `()` to no-arg methods that have side effects; omit `()` on pure
  ones (standard Scala convention).
- Prefer early return for guard conditions over nested `if-else`.
- Don't reach for `SparkSession.active`; pass `SparkSession` explicitly
  as a parameter.

### Naming

- Names describe purpose, not type or inheritance. Avoid `line`, `clue`,
  `principal`, and similar generic terms that inherit a misleading
  meaning.
- No abbreviations (`currentUser`, not `currUser`). Applies to fields,
  enum constants, parameters, and config values.
- Avoid generic factory names like `from(...)` — prefer
  `fromConfig`/`fromUserInput` so call sites read clearly.
- Names must reflect scope. Don't name a connection-level counter
  `totalRows` if it actually means `rowsForThisOperation`.
- No magic numbers or magic strings. Extract to named constants; for
  enums use `MyEnum.X.toString` rather than hardcoded string literals.

### Comments

- Explain *why*, not *what*. Self-explanatory code does not need a
  comment.
- Comment intentional omissions (`// no check needed because ...`) and
  surprising decisions (`// kept until <issue> is fixed upstream`).
- TODO/FIXME comments should link to a tracking issue when the work is
  deferred.

### Error Handling

- Throw for unsupported features and invalid configs; do not silently
  ignore.
- Don't `process.destroy()` engines — notify them to shut down so
  shutdown hooks run.
- Catch the specific exception (e.g. `IOException`,
  `JsonProcessingException`), not its parent.
- Log a warning (not an exception) only for genuinely non-critical
  recoverable failures.

### Logging

- Log levels: `error` for actionable failures only; `warn` for
  recoverable degradation; `info` for lifecycle events users care
  about; `debug` for everything else.
- Use SLF4J placeholders, not string concatenation:
  `log.info("opened session {}", sessionHandle)`.

## Configuration

Configs are `ConfigEntry` instances in `KyuubiConf` (or per-engine
config classes).

- Add new entries to the right config class. Server-only entries belong
  in `serverOnlyConfEntries`.
- `version()` on `ConfigEntry` must be the release the key first ships
  in (not a placeholder, not the future major).
- Operation-level configs go under `kyuubi.operation.*`. Session-level
  under `kyuubi.session.*`. Engine-internal under
  `kyuubi.<engine>.*`. Match the namespace to the scope.
- Provide a sensible default; if no safe default exists, leave it
  undefined and document the requirement (don't invent a misleading
  fallback).
- After adding/modifying any entry, run `dev/gen/gen_all_config_docs.sh`
  to regenerate `docs/configuration/settings.md`.
- Initialize config-dependent fields in `initialize(conf)`, not at
  declaration time.
- Reuse the existing `fallbackConf` chain to share defaults across
  frontend implementations; don't copy identical entries.

## Testing

- Add tests in the same PR as the feature. "Tests will follow in a
  follow-up" is rejected.
- Place tests in the **existing** suite for the area (e.g.
  `KyuubiOperationPerConnectionSuite`,
  `org.apache.kyuubi.operation.PlanOnlyOperationSuite`). Create a new
  suite only when the domain is genuinely new.
- Use `withSessionConf` from `HiveJDBCTestHelper` for per-test config
  overrides. Don't modify shared fixtures or class-level defaults — it
  leaks between tests.
- ScalaTest style for Scala tests; do not use JUnit `@Test`
  annotations.
- AssertJ for Java tests (`assertThat`, `assertThatThrownBy`).
- A passing test that still passes when the change under test is
  reverted is not a test. Verify by reverting locally.
- No `Utils.isTesting` branches in production code. Configure
  test-specific behavior via test config or build properties.
- Benchmarks belong in the Spark benchmark framework, not regular
  suites.
- Integration tests that need a real Spark engine require building a
  distribution first: `build/dist`.

## PR Workflow

### Title and description

PR title: `[KYUUBI #NNNN][COMPONENT] Short imperative summary`

- `NNNN` is the linked GitHub issue number (open one first if missing —
  Kyuubi uses GitHub Issues, not JIRA).
- `[COMPONENT]` is optional but encouraged; common tags include
  `[SERVER]`, `[SPARK]`, `[FLINK]`, `[HIVE]`, `[TRINO]`, `[JDBC]`,
  `[KSHC]`, `[AUTHZ]`, `[LINEAGE]`, `[METRICS]`, `[REST]`, `[CTL]`,
  `[DOC]`. Use existing tags from `git log master | head -100` for
  precedent.
- Use `[WIP]` prefix while still iterating.
- Update the title and description if the scope expands during review.
  Both flow into the merge commit message; `git log` is the
  authoritative narrative.

Fill in the template:

- **Why are the changes needed?** — motivation; reviewers read the diff
  for "what".
- **How was this patch tested?** — concrete commands or a description
  of manual testing. If untested, say so explicitly.
- **Generative AI tooling?** — see §AI-Assisted Contributions.

### One PR, one concern

Refactoring goes in its own PR. Don't bundle a refactor with a feature
or bug fix. If an unrelated nit catches your eye while editing, open a
separate PR (or note it as a follow-up issue).

### Investigating CI failures

Kyuubi CI surfaces failures via uploaded test-log artifacts and the
per-job log, not via check-run annotations. Don't download the full
run log — it is many MB. Use `gh run` to scope to failed steps:

```
# Find the workflow run(s) for the PR branch
gh run list -R apache/kyuubi -b <pr-branch> -L 5

# View only the failed steps for a specific run
gh run view <run-id> -R apache/kyuubi --log-failed

# Or zoom in on one job within a run
gh run view <run-id> -R apache/kyuubi -j <job-id> --log-failed
```

For the full test logs of a failed Kyuubi-and-Spark job, download the
matching `unit-tests-log-*` artifact:

```
gh run download <run-id> -R apache/kyuubi -n <unit-tests-log-...>
```

## AI-Assisted Contributions

AI-assisted contributions are welcome under the
[ASF Generative Tooling Guidance](https://www.apache.org/legal/generative-tooling.html).
The PR template already has a disclosure field — fill it in.

- Disclose tools via `Generated-by: <tool name and version>` in the
  PR description and (optionally) in commit trailers. Do **not** use
  `Co-Authored-By: <AI tool>` — co-author is reserved for human
  contributors per the ASF guidance.
- The contributing human author is responsible for the patch — review
  every line as if you had written it. "I had Claude/Copilot/Codex do
  it" is not a defense for incorrect or low-quality code.
- Do not include `Generated by AI`, model names, or other agent
  self-references in source code, comments, commit messages, or in the
  PR title. Disclosure happens in the PR description, not the code.
- Verify license compatibility of any generated content; do not paste
  output that the tool's terms forbid for open-source distribution.

## Boundaries

### Never

- Push directly to `apache/kyuubi`. All changes go through a PR.
- Force-push to a shared branch. Force-push to your own PR branch is
  fine.
- Break Thrift wire compatibility (the Hive TCLIService schema Kyuubi
  speaks; see §Hard Boundaries). Adding optional fields is OK;
  removing or renumbering is not.
- Commit a `kyuubi_state_store.db`, log files, generated configs, IDE
  files, or credentials. Check `.gitignore` if unsure.
- Skip `dev/reformat` or git hooks (`--no-verify`).
- Use `process.destroy()` to kill an engine — notify it instead.
- Reference `SparkSession.active` — pass `SparkSession` explicitly.

### Ask first

- New runtime dependencies (especially with non-ASL2-compatible
  licenses; `LICENSE-binary` and `dev/dependencyList` must be updated).
- Public API changes (`kyuubi-common`, REST endpoints, Thrift IDL).
- Cross-engine refactors that touch more than one `externals/*` module.
- New Kyuubi configuration entries that gate user-visible behavior.
- New subsystems, large architectural shifts, or anything that warrants
  a KPIP (`kpip.md`). Surface on the dev mailing list first.
- Anything in the high-sensitivity areas above.

## Repository Layout (quick reference)

```
build/                  Maven wrapper, dependency tooling
charts/                 Helm charts
conf/                   Default kyuubi-defaults.conf etc.
dev/                    Contributor scripts (reformat, dependency, gen)
docker/                 Docker assets
docs/                   User and contributor documentation
externals/              Engine implementations
extensions/             Optional plug-ins (server, spark, flink)
integration-tests/      Cross-module integration suites
kyuubi-*/               Server core and shared libraries
.github/                CI workflows, PR/issue templates
```

For anything not covered here, see the
[Contributor Guide](https://kyuubi.readthedocs.io/en/master/contributing/code/index.html)
or ask on the [Apache Kyuubi mailing list](https://kyuubi.apache.org/mailing_lists.html).
