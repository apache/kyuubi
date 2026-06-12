## Context

Kyuubi is a multi-tenant gateway fronting Spark (and other) engines via Thrift/HiveServer2, REST, and MySQL protocols. Apache Spark 3.4 introduced Spark Connect — a gRPC-based client-server protocol using Protocol Buffers — that decouples client code from the Spark driver. PySpark users now connect with `spark.remote("sc://host:port/")` instead of embedding a local SparkSession.

Currently Kyuubi has no gRPC listener and cannot accept Spark Connect clients. The engine side already supports `--spark-connect` mode (Spark 3.4+), which starts an embedded gRPC server inside the engine process. This design adds a **Spark Connect frontend** to Kyuubi that acts as an authenticated, multi-tenant proxy between Spark Connect clients and engine-side Spark Connect servers.

**Constraints:**
- Must not violate the `kyuubi-server` → `externals/*` dependency boundary.
- Must preserve existing Thrift/REST/MySQL surfaces unchanged.
- Protobuf and gRPC dependencies must be profile-gated (`-Pspark-connect`).
- Proto API varies between Spark 3.4, 3.5, and 4.0; implementation must be version-aware.

## Goals / Non-Goals

**Goals:**
- Expose a gRPC endpoint on a configurable port (`kyuubi.frontend.spark.connect.bind.port`, default 15002) implementing `SparkConnectService`.
- Authenticate gRPC connections using Kyuubi's existing auth plugin interface.
- Map each Spark Connect `sessionId` onto a `SparkConnectSession` in Kyuubi's `SessionManager`, with user isolation and resource tagging.
- Transparently proxy `ExecutePlan`, `AnalyzePlan`, `Config`, `Interrupt`, and `ReattachExecute` RPCs to the Spark engine running in `--spark-connect` mode.
- Discover the engine's Spark Connect gRPC port via Kyuubi's existing engine service-discovery mechanism (ZooKeeper / Kubernetes label).
- Support TLS for the client-facing gRPC channel, consistent with Thrift frontend.

**Non-Goals:**
- Implementing a Spark Connect-aware query planner or optimizer inside Kyuubi itself.
- Supporting `AddArtifacts` (client-streaming file upload) in the initial implementation.
- Supporting Spark versions before 3.4 for Spark Connect mode.
- Replacing or modifying the Thrift, REST, or MySQL frontends.
- Supporting Spark Connect in Kyuubi's embedded/local engine mode (development convenience; deferred).

## Decisions

### D1: Proxy architecture (not embed)

**Decision:** Kyuubi relays Spark Connect protobuf streams between the client and the Spark engine process. The engine runs `--spark-connect` mode and handles all plan execution. Kyuubi adds auth, routing, session lifecycle, and multi-tenancy.

**Rationale:** Embedding a SparkSession inside `kyuubi-server` would violate the hard boundary between server and engine modules and prevent the multi-engine / per-user isolation that is Kyuubi's core value. The proxy pattern is consistent with how Kyuubi handles Thrift — Kyuubi is the protocol gateway, not the compute layer.

**Alternatives considered:**
- *Embed SparkSession in server*: Simpler for single-user scenarios, but breaks multi-tenancy and the module boundary. Rejected.
- *Sidecar proxy (separate process)*: Adds operational complexity with no benefit over in-process forwarding. Rejected.

### D2: New Maven module `kyuubi-spark-connect`

**Decision:** All gRPC/protobuf code lives in a new module `kyuubi-spark-connect/`, built only with `-Pspark-connect`. The module depends on `kyuubi-server` for session/operation APIs, not the reverse.

**Rationale:** Keeps protobuf/gRPC off the classpath for deployments that don't need Spark Connect. Mirrors how Kyuubi already gate optional frontends (MySQL).

**Alternatives considered:**
- *Add to kyuubi-server directly*: Makes gRPC a mandatory dependency; harder to version-isolate proto stubs. Rejected.
- *New `extensions/server/kyuubi-spark-connect`*: Possible, but the frontend lifecycle (start/stop with KyuubiServer) fits better as a first-class module than an extension. Deferred for community discussion.

### D3: Proto version strategy — Spark 3.5 first, multi-version via profiles

**Decision:** Target Spark 3.5 `spark-connect-common` proto initially, gated under `-Pspark-3.5` (already the default). Spark 4.0 support added in a subsequent profile. Source-level differences isolated to a versioned shim layer (`SparkConnectProtoShim`).

**Rationale:** Spark 3.5 is the most-deployed LTS version. Proto between 3.4 and 3.5 changed substantially (new fields, renamed messages). Trying to compile a single proto for all versions is not feasible; the shim approach is consistent with how Kyuubi already handles Spark API differences.

### D4: Session identity — Spark Connect `sessionId` → Kyuubi `SessionHandle`

**Decision:** The Spark Connect `UserContext.user_agent` carries the `sessionId` UUID. Kyuubi creates a `SessionHandle` derived from this UUID (or generates one if absent) and stores it in `SessionManager`. Subsequent RPCs from the same `sessionId` reuse the same session handle and thus the same engine connection.

**Rationale:** Spark Connect sessions are long-lived (unlike individual queries); mapping them to Kyuubi sessions enables existing resource limits, event logging, and audit to work without modification.

### D5: Engine port discovery via service registry

**Decision:** When the Spark engine starts in `--spark-connect` mode, it registers its Spark Connect gRPC port in Kyuubi's engine service registry (ZooKeeper node or Kubernetes label `kyuubi.apache.org/spark-connect-port`). `SparkConnectEngineProxy` reads this at session-open time.

**Rationale:** Kyuubi already has a service-discovery layer for Thrift ports; extending it with a second port keeps discovery consistent and avoids hard-coding assumptions about engine-side ports.

**Alternative:** Parse the engine's log output for the port — brittle and not compatible with Kubernetes. Rejected.

### Architecture Diagram

```
Client (PySpark / Go / Rust)
       │  gRPC (SparkConnectService proto)
       │  metadata: Authorization, x-kyuubi-session-id
       ▼
┌──────────────────────────────────────────────────────┐
│  kyuubi-spark-connect module (in kyuubi-server JVM)  │
│                                                      │
│  NettyGrpcServer (port 15002)                        │
│    └─ AuthInterceptor (maps token → KyuubiUser)      │
│         └─ SparkConnectFrontendService               │
│              └─ SparkConnectSessionManager           │
│                   └─ SparkConnectSession             │
│                        └─ SparkConnectEngineProxy    │
│                             (gRPC channel to engine) │
└──────────────────────────────┬───────────────────────┘
                               │  gRPC (SparkConnectService proto)
                               ▼
                  Spark Engine Process
                  (--spark-connect, port auto-assigned)
```

## Risks / Trade-offs

- **gRPC streaming backpressure** → Mitigation: Use `io.grpc.stub.StreamObserver` flow control; implement a bounded queue between inbound client stream and outbound engine channel. Fail-fast on buffer exhaustion rather than OOM.
- **Proto version churn (Spark 3.5 → 4.0)** → Mitigation: `SparkConnectProtoShim` interface isolates version-specific field access; new profiles add new shim implementations.
- **Kyuubi restart loses session mapping** → Mitigation: Document this as a known limitation for v1; Spark Connect clients already handle `DISCONNECTED_PLAN_WITH_NO_COMPLETION_SIGNAL` by reconnecting via `ReattachExecute`. Session state recovery is a follow-up.
- **Netty version conflict** (`grpc-netty-shaded` bundles its own Netty)** → Mitigation: Use `grpc-netty-shaded` (not bare `grpc-netty`) to avoid classpath conflicts with the Thrift frontend's Netty. Validate with `mvn dependency:tree`.
- **`AddArtifacts` deferred** → Clients using `sc.addPyFile()` / `sc.addJar()` will get `UNIMPLEMENTED` in v1. Document clearly; implement in a follow-up.

## Migration Plan

1. The feature is **additive and off by default** (`kyuubi.frontend.spark.connect.enabled=false`).
2. Build is **profile-gated** (`-Pspark-connect`); no impact on existing binary distributions.
3. Users opt in by setting `kyuubi.frontend.spark.connect.enabled=true` and starting Kyuubi with a build that includes the module.
4. Spark engines must be configured to start in `--spark-connect` mode; existing engines without this flag will have Spark Connect sessions fail with a clear error at session-open time.
5. **Rollback**: Disable the config key or remove the module from the classpath. No schema or protocol changes affect existing frontends.

## Open Questions

1. Should the initial implementation support `ReattachExecute` (needed for long-running streaming queries), or defer to a follow-up?
2. Is there a community interest in running Spark Connect in Kyuubi's embedded local mode for CI/testing? If so, what is the minimal shim needed?
3. How does the Spark Connect auth token model interact with Kyuubi's Ranger/Hive Metastore auth plugins — does the token carry the same identity used for SQL authorization?
4. Should `kyuubi-spark-connect` be an extension (under `extensions/server/`) to allow community distribution as a separate artifact, or is it better as a first-class module under `kyuubi-server`'s parent?
