<!--
- Licensed to the Apache Software Foundation (ASF) under one or more
- contributor license agreements.  See the NOTICE file distributed with
- this work for additional information regarding copyright ownership.
- The ASF licenses this file to You under the Apache License, Version 2.0
- (the "License"); you may not use this file except in compliance with
- the License.  You may obtain a copy of the License at
-
-   http://www.apache.org/licenses/LICENSE-2.0
-
- Unless required by applicable law or agreed to in writing, software
- distributed under the License is distributed on an "AS IS" BASIS,
- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
- See the License for the specific language governing permissions and
- limitations under the License.
-->

# Paranoid Mode: Fail-Closed Handling of Unclassified Plan Nodes

**Status:** Implemented. Runtime mechanism in `ParanoidMode.scala` / `PrivilegesBuilder.scala`;
build-time checks in `ClassificationCoverageSuite`; user-facing configuration documented in
`docs/security/authorization/spark/install.md`.

## 1. Background and Motivation

The authz plugin is the enforcement point for fine-grained access control on Spark SQL. It
works by walking each Catalyst logical plan, pattern-matching plan nodes against a set of
known node types (the JSON spec files, plus a few `nodeName` string matches), and building
Ranger access requests from the nodes it recognizes.

The structural weakness is that **recognition is the security boundary, and non-recognition
fails open**. Any plan node that falls through the pattern match is implicitly classified as
not-authz-relevant and contributes no access request. Spark's logical plan space is open —
new commands and relations appear in every Spark minor release, and third-party catalogs
(Iceberg, Delta, Hudi, Paimon) inject their own nodes at user runtime — so the set of
unrecognized nodes grows silently over time.

This is not hypothetical. During the Spark 4.1 porting effort, several cases were found
where adjusting the pattern match converted false-negative authorization decisions into
true positives — found by accident, in the course of other work. And when the build-time
enumeration check (§6) first ran, it counted **136 unclassified authz-relevant plan classes
on the Spark 3.5 classpath and 170 on Spark 4.1** — the population that fail-open behavior
had been hiding.

Merely running the existing test suite in deny mode then surfaced two live gaps on master
that green CI had been certifying for years:

- **Iceberg metadata tables were never authorized.** `SELECT * FROM t.snapshots` produces a
  `DataSourceV2Relation` whose table reports a four-part name; the extractor threw a
  `MatchError` that vanished into the fail-open path, so metadata reads (snapshot history,
  manifests, partition statistics) required no privilege at all. Deny mode turned the
  swallowed exception into a violation, and the fix authorizes metadata reads as reads of
  the base table.
- **Connector-planned scans were skipped.** Iceberg's MERGE INTO rewrite embeds an
  already-planned `DataSourceV2ScanRelation` — the rewrite's own read of the target table —
  a leaf the builder did not recognize and silently skipped. The named source and target
  objects were still checked through the command spec, so every test kept passing; the
  embedded scan is exactly the kind of node nobody thought to test. It is now classified
  with its own scan spec.

A green test suite does not bound this risk: every test was written for a node type someone
had already classified, so the suite certifies that the *previously known* sample still
authorizes correctly and says nothing about nodes nobody thought to test.

The stakes are what make this urgent. Deployments that take the plugin's security claims at
face value put regulated workloads (PII, PHI, financial data) behind it; for them a
fail-open authorization gap is not a bug ticket but a potential compliance incident, and
the failure mode is silent by construction. The gap between "policies are enforced on every
operation the plugin recognizes" and "policies are enforced on every operation" is
precisely the part an operator cannot audit from the outside.

## 2. The Fail-Open Layers

Four distinct layers, all addressed by this change. The first three were identified up
front; the fourth was discovered during implementation.

1. **Unknown commands.** `PrivilegesBuilder.buildCommand` dispatches on class name against
   the three command spec maps. The fallthrough returned `OperationType.QUERY` with zero
   privilege objects, so `RuleAuthorization` built zero access requests and never called
   `verify` — the command executed with no check at all.

2. **Unknown leaf relations.** In `buildQuery`, a leaf node not matched by a scan spec (or
   the `UnresolvedRelation` nodeName match) fell into the generic recursive arm, has no
   children, and contributed nothing. A *known* scan node that was not `resolved` fell
   through the same way.

3. **Extractor drift on known commands.** Descriptor extraction was wrapped in
   `catch { case e: Exception => LOG.debug(...); Nil }`. A command that *has* a spec still
   failed open when its extractors broke against a new Spark version — the class name
   matched, but the field the extractor reads had moved. The spec's existence created the
   appearance of coverage; the only trace was a DEBUG log line.

4. **Constant-projection pruning.** `buildQuery` deliberately skips the subtree of a
   `Project` whose output has no relation to its input (a constant projection reads no
   columns). But the subtree still *executes* — `SELECT 'x' FROM t` — so an unclassified
   node under a constant projection was invisible to privilege building entirely.

   The sweep added here (§4.2) closes the *classification* half of this: a node under a
   constant projection is now seen and reported. It does not close the *privilege* half.
   `SELECT 1 FROM protected_table` still produces no privilege object for the table, so no
   table-level authorization request is made for it, exactly as before this change. That is
   a pre-existing gap in privilege building, not in classification, and closing it means
   emitting a table-level (columnless) privilege object for the pruned subtree — a change
   to what queries are allowed, which belongs in its own PR.

### Case study: CALL on Spark 4

The sharpest real example, verified against the Spark 4.1 classpath. On Spark 3.x with
Iceberg, `CALL` resolves to `org.apache.spark.sql.catalyst.plans.logical.Call` — a class
Iceberg injects, extending Catalyst's `Command`, so dispatch reaches the spec in
`table_command_spec.json` and authorizes the call. Spark 4.x ships *its own* `Call` under
the **identical fully-qualified class name** with a different type hierarchy: a `UnaryNode`
implementing `ExecutableDuringAnalysis` — not a `Command`. Dispatch never reached the spec
lookup; the procedure executed *during analysis*, before any optimizer-phase check could
run. The spec entry still existed and still named the right class; it was simply
unreachable.

Three properties make this the canonical motivating case:

- **Drift by type hierarchy under a stable class name.** Neither a class-name check nor the
  presence of a spec entry detects it.
- **It is neither a `Command` nor a `LeafNode`.** An invariant scoped to those two shapes
  would have missed it — which is why the invariant (§3) has its third clause, and why the
  build-time enumeration (§6) covers `ExecutableDuringAnalysis`.
- **No runtime check in this plugin can stop it.** `RuleAuthorization` is an optimizer
  rule; an analysis-time-executable node has already run by then. The build-time check is
  the only net for this shape, and it currently reports `Call` as an **acknowledged,
  tracked vulnerability** on Spark 4 (§6). Closing it requires an analysis-time rule or
  blocking the operation on Spark 4 outright.

## 3. The Invariant

Naively flagging every fallthrough would be unusably noisy: intermediate operators
(`Project`, `Filter`, `Join`, …) legitimately recurse, because privileges are carried by
leaves and commands. The invariant paranoid mode enforces is narrower:

> Every `Command`, every `LeafNode`, every node that can execute or mutate state outside
> the checked path (e.g. Spark 4's `ExecutableDuringAnalysis`), and every node whose class
> name has a spec that dispatch did not consult, encountered during privilege building —
> including in subtrees pruned by constant-projection elimination — must be either
> (a) matched by a spec, or (b) present on the explicit allowlist. Ordinary non-leaf query
> operators recurse freely.

The honest caveat: "side-effecting non-leaf, non-command node" is not a category Catalyst
exposes as a single stable supertype. `ExecutableDuringAnalysis` covers the case we know
about; nothing guarantees a future Spark version won't introduce another. This is a core
reason the runtime deny mode cannot be replaced by build-time checks alone — the set of
dangerous shapes is open, so the only sound default is "unrecognized ⇒ deny," not
"unrecognized-and-matching-a-known-dangerous-supertype ⇒ deny."

Additionally:

> An extraction failure against a matched spec is a violation **when no descriptor of the
> command completes at all** (§5). A single descriptor failing while a sibling succeeds is
> expected version variance, not drift.

## 4. Runtime Design

### 4.1 Configuration

```
spark.kyuubi.authz.unclassifiedNode.behavior = allow | warn | deny   (default: warn)
```

- **allow** — legacy behavior, for deployments that cannot tolerate new noise. Violations
  are still counted and visible at DEBUG.
- **warn** — enforce nothing, but log at WARN once per (class name, violation kind) per
  JVM, and count every occurrence. This is the accretion mode used to build the allowlist
  from real workloads.
- **deny** — throw `AccessControlException` naming the unclassified class and the config
  key. This is the mode documented for regulated deployments, and the mode every authz
  test suite runs in (`SparkSessionProvider` sets it unconditionally).

An invalid value fails loudly (`IllegalArgumentException`) rather than defaulting: a
security knob that silently absorbs typos is itself a fail-open.

Violation kinds (see `ParanoidMode.ViolationKind`): unclassified command, unclassified
leaf, unresolved known scan, unreachable spec, analysis-time execution, extraction failure.

### 4.2 Dispatch hardening

`PrivilegesBuilder.build` gained a fallback arm: any node whose class name has a command
spec routes to `buildCommand` even if it is not a `Command` on this Spark version. This is
the direct, general fix for the CALL supertype-drift shape — the spec becomes reachable by
membership, not by supertype. (For `CALL` specifically it is still too late at optimizer
time, per §2; the arm exists so the *next* drift of this shape degrades to an ordinary
spec-driven check instead of silence.)

### 4.3 Cached relations

`InMemoryRelation` was an allowlist candidate on the reading that "the originating plan was
authorized when the cache was populated". It is not: `CacheManager` lives in `SharedState`
and answers every session in the engine, so who populated an entry says nothing about who
may read it. Cache substitution happens in `CacheManager.useCachedData`, which runs *before*
the optimizer, and `RuleAuthorization` is an optimizer rule — so a second user's identical
query arrives at privilege building as a bare cached leaf, with every relation the query
read already collapsed away. Allowlisting it would have made "user A cached it" a working
grant to user B.

`buildQuery` therefore has a dedicated arm: it recovers the analyzed plan the entry was
built from out of the CacheManager and authorizes *that*, so the reader is asked for
exactly the privileges the cached query itself required. Entries are matched on the cache
builder, not the relation, because `useCachedData` hands the optimizer a copy with its
output re-mapped onto the fragment it replaced. If the lookup finds nothing, the node is
reported as an extraction failure and paranoid mode applies — a cached read whose origin
cannot be established fails closed under `deny`.

### 4.4 The allowlist

`known_harmless_spec.json` lives alongside the command spec files, is loaded the same way,
and is maintained by the same generator (`KnownHarmlessNodes` in the test tree, written by
`JsonSpecFileGenerator`). Entries are exact class names with a **required `reason` field**
— enforced by a `require` in `HarmlessNodeSpec` — so each entry is a reviewed decision an
auditor can read, not a reflexive silencing. Each entry also names the exact Spark minor
versions its review applies to (§4.6); on any other version the entry is inert.

Three patterns emerged during triage that future entries should be checked against:

- **"Reads no stored data"** — `LocalRelation`, `OneRowRelation`, `Range`,
  `CTERelationRef`, `CommandResult`, session-conf commands. The straightforward kind.
- **"Enforced elsewhere"** — v2 `ShowNamespaces` / `ShowTables`. These are row-filtered by
  the `ObjectFilterPlaceHolder` + `FilterDataSourceV2Strategy` machinery, but Spark's
  `QueryExecution.eagerlyExecuteCommands` *also* executes the bare inner command in a
  nested QueryExecution whose unfiltered result the placeholder deliberately discards
  (`withNewChildInternal` refuses child swaps that change `nodeName`). That nested run hits
  `RuleAuthorization` with the bare command as root and **must be allowed** — enforcement
  lives in the placeholder machinery, not in `PrivilegesBuilder`. Denying it breaks every
  SHOW query. This subtlety is load-bearing; do not "fix" it.
- **"This plugin's own machinery"** — the `FilteredShow*Command` wrappers the plugin
  installs in place of the v1 SHOW commands. They are `Command`s by shape, handled by
  dedicated dispatch arms in `PrivilegesBuilder.build`, with per-row access checks inside
  the wrapper itself.

Allowlisting a `Command` is higher-stakes than allowlisting a leaf relation, so it takes a
second, colocated review: the entry must also be exempted by name in
`ClassificationCoverageSuite`'s "not Commands in disguise" test, forcing every such
addition to touch the coverage suite where the reviewer sees the policy.

`nodeName`-string matching is not extended: allowlist and specs key on fully qualified
class names only. The existing `nodeName == "UnresolvedRelation"` match stays and counts
as classified.

### 4.5 Extraction-failure semantics (layer 3)

Specs are written so an object "wins at least once" across Spark versions and command
shapes: a command may carry several descriptors for the same object, of which only one is
expected to succeed on any given version. Descriptors also fail legitimately by *shape* —
for Hudi's path-based `CALL` procedures, every table descriptor fails while the URI
descriptors carry the enforcement.

The rule is therefore **per command, not per descriptor or per descriptor family**: a
violation is reported only when at least one descriptor threw and *no descriptor of the
whole command completed* (`DescOutcomes` in `PrivilegesBuilder`). That is the true drift
shape — the spec matches by name but can no longer extract anything from this Spark's plan
layout.

Residual gap, accepted deliberately: partial drift (a broken table descriptor alongside a
still-working query descriptor) is not reported at runtime, because it is
indistinguishable from legitimate shape variance. The build-time checks are the net for
that class of drift.

Two guardrails around the tracking: `AccessControlException` is always rethrown (an
authorization verdict bubbling up from nested privilege building must never be recorded as
an extraction failure), and recursion into extracted queries happens *outside* the tracked
region so nested violations surface as themselves.

The same per-command logic applies to scan specs in `buildQuery` via
`ScanSpec.tablesWithFailures` / `urisWithFailures`: a matched scan that yields no table, no
URI, and at least one exception is a violation.

### 4.6 Version-scoped audits (`verifiedSparkVersions`)

The CALL case (§2) shows that "known" and "harmless" are assertions about a class *on a
specific Spark version*: the class under the same fully qualified name is free to become
something else in the next minor release. Every spec entry — allowlist and command/scan
specs alike — therefore carries a `verifiedSparkVersions` field naming the Spark versions
its review applies to.

The field is an **explicit enumeration of exact `major.minor` pairs, never a range**.
Ranges invite boundary misreadings ("less than 4.0, exclusive" read as inclusive); a list
has no boundary to misread. The format is validated at construction (`SparkVersionAudit`),
and the enumeration gives the right default for free: a new Spark minor is unverified until
a human adds it — which is exactly when the re-review should happen. A version joins an
entry's list by being tested or reviewed, never by interpolation (this is why an entry can
legitimately list `3.5, 4.1` without `4.0`).

The field's force differs by spec kind, deliberately:

- **Allowlist entries gate.** An entry not verified for the running Spark's `major.minor`
  is inert: the node counts as unclassified and paranoid mode applies (fail closed,
  per node). The violation message says so explicitly — "verified for 3.5, 4.0, 4.1 but
  not for 4.2; re-review the entry" is far more actionable than "unclassified". An
  allowlist entry *grants silence*, so its scope must be exactly as wide as its review.
- **Command and scan spec entries are advisory.** A spec still engages on an unverified
  version. A spec *imposes checks*, so staying active on an unaudited version is the safe
  direction — going inert there would fail everything closed and make new Spark versions
  unusable, while actual drift is caught by the extraction-failure tracking (§4.5) and the
  build-time checks (§6). The metadata records what was audited when, and gives the
  build-time tooling a place to grow (e.g. flagging specs engaged far outside their
  audited range).

Where the values come from differs as well. A new spec declares `verifiedSparkVersions` at
its definition site, naming the minors it was actually reviewed against. The command and
scan specs that predate the Spark 4 port instead take theirs from a frozen ledger,
`src/test/resources/spec_verified_spark_versions.txt`, which records the `3.5`
baseline those entries inherited wholesale rather than earned per minor; the ledger's header
explains why that distinction is preserved rather than laundered into as many individual
per-minor claims. The ledger is deliberately closed rather than a default: a spec that
neither declares its own versions nor appears there fails generation, so a newly added spec
cannot quietly inherit the baseline, and an entry left behind by a deleted spec fails the
same check rather than lingering as provenance for nothing.

The build-time enumeration (§6) respects the gate: on each profile, only allowlist entries
verified for that profile's Spark minor count as classified, so porting to a new Spark
version surfaces every entry awaiting re-review in that profile's backlog at PR time.

## 5. Test Posture

All authz test sessions run with `deny` set in `SparkSessionProvider` — fallout goes to a
spec or to the allowlist, never to relaxing the default. Behavior-specific tests live in
`ParanoidModeSuite` (synthetic unclassified leaf/command nodes, all three behaviors, the
constant-projection sweep, allowlist loading and the required-reason rule).

Note a suite green in `deny` mode establishes coverage only of the plans the suite
exercises. It is the enumeration check (§6), not the test suite, that approximates closure
over the classpath.

## 6. Build-Time Coverage Checks

`ClassificationCoverageSuite`, run per Spark profile, catches classification drift at PR
time instead of in production:

1. **Analysis-time reachability.** For every command spec classname resolvable on this
   classpath, assert it does not implement `ExecutableDuringAnalysis` without being a
   `Command` — the shape whose spec can never enforce anything (§2). Known instances live
   in an explicit `acknowledgedGaps` set with tracking comments; an entry there is an
   **acknowledged vulnerability on that Spark version, not a pass**. Currently:
   `logical.Call` on Spark 4.
2. **Allowlist re-review.** Allowlisted classes that are (or became) `Command`s on this
   classpath fail unless explicitly exempted in the suite (§4.4), and no class may have
   both a spec and an allowlist entry.
3. **Enumeration (total accounting).** Scan every code source that can contribute plan
   nodes — the Spark jars, whichever catalog plugins are on this profile's classpath, and
   this plugin's own classes directory (its markers and filtered-SHOW wrappers are plan
   nodes too) — and enumerate **every concrete `LogicalPlan` descendant**. Each class
   lands in exactly one bucket:
   - *spec'd* — a command/scan spec (or nodeName match) builds privileges for it:
     definitively authz-relevant;
   - *allowlisted* — reviewed as harmless for this profile's Spark minor (§4.6);
   - *pass-through* — neither `Command`, `LeafNode`, nor analysis-time-executable:
     `buildQuery` recurses through it, and whatever carries relevance beneath it is
     itself in the enumeration;
   - *dirty laundry* — relevant by shape but neither spec'd nor allowlisted, pinned one
     classname per line in `src/test/resources/classification_backlog_spark_<minor>.txt`
     (currently 135 entries for 3.5, 182 for 4.0, 195 for 4.1, and 208 for 4.2 — the 4.x
     figures grow with each profile's connector versions, since third-party nodes were
     never classified to begin with).

   A companion check keeps the allowlist honest from the other side: an entry whose class
   is a pass-through shape fails the build, because nothing would ever consult it and it
   would read as coverage. A class **new** to the diff fails the build with an actionable
   message: classify it, allowlist it with a reason, or consciously regenerate the backlog.
   A class that leaves the diff must also leave the backlog, so the backlog only ever
   shrinks by being triaged, never silently.

Regeneration: `KYUUBI_UPDATE=1 build/mvn test -pl extensions/spark/kyuubi-spark-authz
-DwildcardSuites=org.apache.kyuubi.plugin.spark.authz.ClassificationCoverageSuite` (spec
and allowlist JSON via `dev/gen/gen_ranger_spec_json.sh`). Note the generator reads specs
from `target/classes`, so regenerate the allowlist first, then the backlog in a second
pass.

These checks do **not** replace runtime paranoid mode, for two independent reasons:
build-time enumeration cannot see third-party catalog plugins loaded only in the user's
environment, and the set of dangerous node shapes is open (§3), so no fixed enumeration is
provably complete.

## 7. Known Limitations and Follow-Ups

- **`CALL` on Spark 4 is outside the fail-closed guarantee.** The procedure runs during
  analysis, before any authorization rule; by the time paranoid mode sees the plan the side
  effect has already happened, and no amount of unwrapping at optimizer time can undo it.
  `deny` therefore does *not* make `CALL` fail closed on Spark 4 — it reports after the
  fact. Only build-time check §6.1 names the gap. Closing it needs an analysis-time rule or
  a hard block on Spark 4; tracked via the `acknowledgedGaps` entry.
- **A startup Spark-major-version assertion** (refuse to initialize on an unsupported
  Spark major) is a cheap, orthogonal hardening recommended for released branch lines: the
  released jar today loads on Spark 4 and *visibly enforces* policies on ordinary
  statements while silently allowing `CALL` — apparent enforcement plus a silent gap, in
  exactly the deployment the docs disclaim.
- **Partial extractor drift** with a surviving sibling descriptor is not reported at
  runtime (§4.5).
- **Row-filter and data-masking rule paths** (`RuleApplyRowFilter`,
  `RuleApplyDataMaskingStage0/1`) share the recognition machinery but have their own
  traversals; they are not covered by this change and should get the same treatment as a
  follow-up.
- **Out of scope by design:** wrong (as opposed to missing) classification in existing
  specs; physical-plan and RDD-level escape hatches (`df.rdd`, `ExternalRDD` /
  `LogicalRDD` are allowlisted with exactly this reasoning); subquery-expression traversal.
- **The backlogs are triage lists, not archives.** 136 + 170 classes await a
  spec-or-allowlist decision each; allowlist review must be part of every Spark version
  bump, since a node harmless today can gain authz-relevant behavior in a later release.

## 8. Open Questions

1. Should `deny` eventually be the default? Fail-closed-by-default is the defensible
   posture for a security plugin, but it changes behavior for every existing deployment.
   Shipping with `warn` first is the safe rollout; revisit after a release of soak time.
2. Granularity: is a single behavior knob enough, or do the violation kinds need
   independent settings? (Current position: one knob until real usage proves otherwise.)
3. This changes user-visible security posture, so it should land discuss-first: framed as
   "deny-by-default mode for unclassified plan nodes," with a search of existing issues
   and discussions for prior art before opening a new one.

