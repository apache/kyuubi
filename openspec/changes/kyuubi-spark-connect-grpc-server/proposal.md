## Why

Spark Connect (GA in Apache Spark 3.4) defines a language-agnostic gRPC protocol that decouples clients from the Spark driver. Today Kyuubi only exposes Thrift/JDBC/REST surfaces; PySpark users connecting via `spark.remote("sc://...")` or any non-JVM thin client cannot benefit from Kyuubi's multi-tenancy, session pooling, and access control. Supporting Spark Connect transforms Kyuubi into a unified gateway for both legacy JDBC workloads and modern DataFrame/Spark Connect clients, with no changes required on the Spark engine side.

## What Changes

- **New gRPC listener** in `kyuubi-server` implementing `SparkConnectService` proto (Apache Spark Connect protocol).
- **New module** `kyuubi-spark-connect` containing the gRPC server, protobuf stubs, and session bridge.
- **Session management extension**: Spark Connect sessions are mapped onto Kyuubi's existing `SessionManager` / `OperationManager` with a new `SparkConnectSession` type.
- **Engine-side proxy**: Kyuubi spawns (or reuses) a Spark application running in `--spark-connect` mode and reverse-proxies protobuf streams between the client and the engine's embedded Spark Connect server.
- **New configuration namespace** `kyuubi.frontend.spark.connect.*` for gRPC port, TLS, max-message-size, etc.
- Regenerated `docs/configuration/settings.md` for new config entries.

## Capabilities

### New Capabilities
- `spark-connect-frontend`: Kyuubi exposes a gRPC server endpoint implementing the Spark Connect wire protocol (`SparkConnectService`) for client connections.
- `spark-connect-session`: Lifecycle management (create / reuse / close) of Spark Connect sessions within Kyuubi's `SessionManager`, with user isolation and resource tagging.
- `spark-connect-engine-proxy`: Transparent forwarding of Spark Connect protobuf streams (Execute, Analyze, Config, Interrupt) from Kyuubi to a per-user Spark engine running in Spark Connect server mode.
- `spark-connect-auth`: Authentication and authorization for gRPC connections (token-based, matching Kyuubi's existing auth plugin interface).

### Modified Capabilities
<!-- No existing spec-level requirements change; Thrift/JDBC/REST surfaces are unaffected. -->

## Impact

- **New module**: `kyuubi-spark-connect/` (or `extensions/server/kyuubi-spark-connect/`) with gRPC and protobuf dependencies.
- **Dependencies added**: `grpc-netty-shaded`, `protobuf-java`, `spark-connect-common` (from Spark 3.4+); gated behind a Maven profile `-Pspark-connect`.
- **`kyuubi-server`**: `FrontendService` abstraction extended; Netty gRPC channel added alongside existing Thrift Netty channel; startup wiring in `KyuubiServer`.
- **`kyuubi-common`**: `SessionManager` and `OperationManager` accept the new `SparkConnectSession` / `SparkConnectOperation` subtypes.
- **Kubernetes / HA**: Engine discovery must handle `SparkConnectServerUrl` alongside existing `ThriftServerUrl`; Spark Connect uses port 15002 by default.
- **Configuration docs**: `dev/gen/gen_all_config_docs.sh` must be re-run after adding new `KyuubiConf` entries.
- **License**: `spark-connect-common` proto files are Apache 2.0; confirm `LICENSE-binary` and `NOTICE` before shipping.
