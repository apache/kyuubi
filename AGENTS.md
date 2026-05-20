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

A guide for AI coding agents contributing to Apache Kyuubi. Pairs with `CONTRIBUTING.md` and the [Contributor Guide](https://kyuubi.readthedocs.io/en/master/contributing/code/index.html).

## Pre-flight Checks

Before the first edit or test in a session:

1. Run `git remote -v`. An `apache` remote must point to `apache/kyuubi`; do not work from a fork-only checkout.
2. If `apache/master` is stale, `git fetch apache master` before branching or rebasing.
3. Check `git status`. If the tree is dirty, ask the user to stash before any branch switch or release/RAT-style check.
4. Existing PR: resolve the branch via `gh api repos/apache/kyuubi/pulls/<num> --jq '.head.ref'`, then check it out.
5. New work: branch from `apache/master`. Branch names like `kyuubi-NNNN-short-slug` are convention, not policy.
6. Confirm `git config user.email` matches an email on the GitHub account that will open the PR.
7. Keep private/ignored files outside the repo root — RAT scans generated local files even when `.gitignore`d.

## Architecture

Kyuubi is a multi-tenant gateway that fronts pluggable SQL engines (Spark, Flink, Trino, JDBC, etc.). The module split is load-bearing in code review.

### Module Layout

- `kyuubi-server/` — gateway process. Must not depend on engine modules under `externals/` or implement engine-side runtime behavior.
- `kyuubi-common/` — shared service abstractions, config registry, session/operation base classes, relocated Hive Thrift RPC.
- `kyuubi-ha/`, `kyuubi-events/`, `kyuubi-metrics/`, `kyuubi-rest-client/`, `kyuubi-zookeeper/` — focused libraries.
- `kyuubi-ctl/` — admin CLI.
- `kyuubi-hive-jdbc/`, `kyuubi-hive-beeline/` — JDBC client and shell.
- `externals/kyuubi-{spark,flink,hive,trino,jdbc,data-agent}-sql-engine/` — engine processes. Must not depend on each other.
- `extensions/{server,spark,flink}/` — opt-in plug-ins.
- `integration-tests/` — cross-module suites.
- `kyuubi-assembly/` — binary distribution.

### Hard Boundaries

- Server must not depend on Kyuubi engine modules or use engine runtime APIs (Spark/Flink internals) to implement engine behavior server-side.
- Protocol/client libraries are not a blanket exception — if one is needed in `kyuubi-server`, keep it limited to wire-protocol or client-model translation and verify the dependency tree.
- Engines must not depend on sibling engines (e.g. no `kyuubi-flink-sql-engine` → `kyuubi-spark-sql-engine`).
- Public APIs are abstract over the cluster manager. Use `killApplication`, not `closeYarnJob`; APIs, classes, and config keys must work for YARN, Kubernetes, and future managers.
- Spark SQL syntax extensions need maintainer agreement, version-aware parser support, and a clear reason they cannot stay upstream-compatible.
- Do not break the Thrift wire protocol. Kyuubi speaks Hive TCLIService through relocated dependencies; never remove or renumber wire fields.
- Prefer reusing existing Thrift-defined operations with an operation-level config key over extending the wire schema; change the wire schema only with maintainer agreement.

### High-Sensitivity Areas

Get reviewer attention before changing:

- `kyuubi-common/.../session/SessionManager`, `.../operation/OperationManager` — lifecycle, concurrency, shutdown.
- `kyuubi-server/.../engine/KubernetesApplicationOperation` — cluster-manager integration; tests must not assume a real cluster.
- `kyuubi-common/.../config/KyuubiConf` — config registry; changes require regenerating `settings.md`.
- `kyuubi-server/.../api/v1/` — public REST surface; add auth checks before exposing.

## Build and Test

Use the bundled Maven wrapper (`build/mvn`).

```
build/mvn -Pfast clean package -DskipTests           # local compile, skips tests/style/docs/RAT/downloads
build/mvn clean package -pl kyuubi-common -am -DskipTests
build/mvn clean install                              # all tests
build/mvn clean install -pl kyuubi-common -am        # one module's tests
build/mvn test -pl kyuubi-server -am -Dtest=none \
    -DwildcardSuites=org.apache.kyuubi.server.api.v1.SessionsResourceSuite
build/mvn test -pl kyuubi-hive-jdbc -am -Dtest=KyuubiStatementTest -DwildcardSuites=none
```

Use `-am` (also-make) when building or testing a single module — without it, Maven fails unless the dependency's artifact is already in `~/.m2`. Integration-style tests that exercise a packaged engine require `build/dist` first.

### Engine profile matrix

| Profile | Notes |
|---|---|
| `-Pspark-3.5` (default), `-Pspark-{3.3,3.4,4.0,4.1,master}` | Spark version |
| `-Pflink-1.20` (default), `-Pflink-{1.17,1.18,1.19}` | Flink version |
| `-Pscala-2.13` | Scala 2.13 (default is 2.12) |
| `-P{spark,flink,hive}-provided` | skip bundled engine downloads |
| `-Pmirror-cdn` | use Apache mirror CDN for engine archives |
| `-Pfast` | skip tests/style/docs/enforcer/RAT/downloads |

When engine code varies across versions, gate source/binary differences by Maven profile and runtime capability differences by feature detection — not by parsing version strings.

### Style, dependencies, docs

```
dev/reformat                          # Spotless (+ Python Spotless if `black` is installed); run before every commit
build/dependency.sh [--replace]       # detect / update dev/dependencyList drift after dependency changes
dev/gen/gen_all_config_docs.sh        # regenerate docs/configuration/settings.md after KyuubiConf changes
```

Runtime dependency changes may also require `LICENSE-binary` and `NOTICE` updates.

## Coding Conventions

### Languages

- Java and Scala are both first-class. Match the surrounding module's language and idioms rather than introducing the other.
- Scala follows the Scalafmt/Scalastyle config; Java follows Spotless/google-java-format. `dev/reformat` enforces both.

### Scala

- Prefer `Option[T]` over `null`, except for performance-sensitive code paths.
- Pattern-match guards (`case X(...) if cond =>`) over nested `if`/`match`.
- `()` on no-arg methods with side effects; omit on pure methods.
- Early return for guard conditions over nested `if-else`.
- Avoid new `SparkSession.active` lookups in production code; pass `SparkSession` explicitly unless a Spark entry point gives no parameterized path.

### Naming

- Names describe purpose, not type. Avoid generic terms like `line`, `clue`, `principal`.
- No abbreviations (`currentUser`, not `currUser`).
- Avoid generic factories like `from(...)` — prefer `fromConfig`, `fromUserInput`.
- Names must reflect scope: a connection-level counter is not `totalRows` if it counts one operation.
- No magic numbers/strings. Use named constants; for enums use `MyEnum.X.toString`.

### Comments

- Explain why, not what. Self-explanatory code needs no comment.
- Comment intentional omissions and surprising decisions.
- TODO/FIXME should link a tracking issue when the work is deferred.
- When referencing upstream or external issues, include the fixed version or release when known.

### Error handling and logging

- Throw for unsupported features and invalid configs; do not silently ignore.
- Do not introduce direct `process.destroy()` / `destroyForcibly()` for engine shutdown — notify engines so their shutdown hooks run.
- Catch the specific exception (`IOException`, `JsonProcessingException`), not its parent.
- Log levels: `error` = actionable failure, `warn` = recoverable degradation, `info` = lifecycle, `debug` = diagnostics.
- SLF4J placeholders, not concatenation: `log.info("opened session {}", handle)`.

## Configuration

Configs are `ConfigEntry` instances in `KyuubiConf` or per-engine config classes.

- Server-only entries call `.serverOnly` on the builder; `KyuubiConf` tracks them in `serverOnlyConfEntries` so they are stripped before engine-side propagation.
- `version()` on `ConfigEntry` is the release the key first ships in.
- Namespace matches scope: `kyuubi.operation.*`, `kyuubi.session.*`, `kyuubi.<engine>.*`.
- Provide a sensible default; if none is safe, leave it undefined and document the requirement.
- Initialize config-dependent fields in `initialize(conf)`, not at declaration time.
- Reuse `fallbackConf` to share defaults across frontends; do not copy identical entries.
- After any entry change, run `dev/gen/gen_all_config_docs.sh` and commit the diff.

## Testing

- Tests ship in the same PR as the feature. "Tests will follow" is rejected.
- Place tests in the existing suite for the area; create a new suite only when the domain is genuinely new.
- Use `withSessionConf` from `HiveJDBCTestHelper` for per-test config overrides; do not mutate shared fixtures.
- Scala tests use ScalaTest style — no JUnit `@Test` annotations.
- Java tests follow the surrounding module style; inspect existing test dependencies and imports before choosing JUnit APIs or assertions.
- A test that still passes when the change under test is reverted is not a test. Verify meaningful failure locally when practical.
- Do not add new `Utils.isTesting` branches in production code; use test config or build properties.
- Benchmarks belong in the Spark benchmark framework, not regular suites.

## PR Workflow

PR title: `[KYUUBI #NNNN][COMPONENT] Short imperative summary`. `NNNN` is the linked issue number; `[COMPONENT]` is optional but encouraged (use existing tags from recent `git log` for precedent). Use `[WIP]` while iterating. Update title and description if scope expands — both flow into the merge commit.

Fill in `.github/PULL_REQUEST_TEMPLATE`:

- **Why are the changes needed?** — motivation. Reviewers read the diff for "what".
- **How was this patch tested?** — concrete commands or manual steps; explain if tests were not added.
- **Was this patch authored or co-authored using generative AI tooling?** — fill the field.
- Keep prose precise and concise — match length to the size of the change; avoid filler and verbose restating.

One concern per PR. Refactors go in their own PR; if an unrelated nit catches your eye, open a separate PR or follow-up issue.

### Investigating CI failures

Use scoped `gh run` commands instead of downloading full logs first.

```
gh run list -R apache/kyuubi -b <pr-branch> -L 5
gh run view <run-id> -R apache/kyuubi --log-failed
gh run view <run-id> -R apache/kyuubi -j <job-id> --log-failed
gh run download <run-id> -R apache/kyuubi -n <unit-tests-log-...>   # full logs of a failed Kyuubi+Spark job
```

## AI-Assisted Contributions

Allowed under [ASF generative tooling guidance](https://www.apache.org/legal/generative-tooling.html) and Kyuubi's PR template. AI is used for more than code generation (review, reading, solution exploration), so disclose assistance rather than implying the patch was machine-authored.

- Disclose assistance via an `Assisted-by:` trailer in the PR template field. Identify the agent and model so the contribution is traceable. Examples: `Assisted-by: Claude:claude-opus-4-7`, `Assisted-by: Claude Opus 4.7`, `Assisted-by: OpenCode with DeepSeek V4 Pro`.
- Do not list AI tools as co-authors; disclose tool assistance in the PR description.
- The human author is responsible for the patch and must review every line.
- No AI self-references in source, comments, commit messages, or PR titles — disclosure stays in the PR description.
- Verify license/terms compatibility for generated content.

## Boundaries

### Never

- Push directly to `apache/kyuubi`. All changes go through a PR.
- Force-push to a shared branch. Force-push to your own PR branch is fine when coordinated.
- Commit `kyuubi_state_store.db`, logs, generated local configs, IDE files, credentials, or other private artifacts.
- Skip `dev/reformat` before committing.
- Introduce new direct `process.destroy()` / `destroyForcibly()` for engine shutdown.
- Introduce new `SparkSession.active` lookups in production code without a reviewer-approved reason.

### Ask first

- New runtime dependencies, especially with non-ASL2-compatible licenses.
- Public API changes (`kyuubi-common`, REST endpoints, Thrift IDL).
- Cross-engine refactors that touch more than one `externals/*` module.
- New configuration entries that gate user-visible behavior.
- New subsystems or large architectural shifts — discuss on GitHub Discussions or the dev mailing list when scope warrants community design input (GitHub activity is mirrored to the ASF mailing list archives).
- Anything in High-Sensitivity Areas above.

For anything not covered here, see the [Contributor Guide](https://kyuubi.readthedocs.io/en/master/contributing/code/index.html), [GitHub Discussions](https://github.com/apache/kyuubi/discussions), or the [Apache Kyuubi mailing list](https://kyuubi.apache.org/mailing_lists.html).
