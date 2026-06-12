## ADDED Requirements

### Requirement: gRPC server lifecycle
Kyuubi server SHALL start a Netty-based gRPC server implementing `SparkConnectService` when `kyuubi.frontend.spark.connect.enabled=true`. The server SHALL bind to `kyuubi.frontend.spark.connect.bind.host` on port `kyuubi.frontend.spark.connect.bind.port` (default 15002). The gRPC server SHALL start and stop as part of `KyuubiServer` lifecycle (alongside the Thrift and REST frontends) and SHALL not affect startup of other frontends when it fails to bind.

#### Scenario: Server starts with config enabled
- **WHEN** `kyuubi.frontend.spark.connect.enabled=true` and the bind port is available
- **THEN** the gRPC server SHALL bind successfully and log the effective listen address at INFO level

#### Scenario: Server disabled by default
- **WHEN** `kyuubi.frontend.spark.connect.enabled` is not explicitly set
- **THEN** no gRPC port is opened and no gRPC-related threads are started

#### Scenario: Bind port unavailable
- **WHEN** `kyuubi.frontend.spark.connect.enabled=true` and the port is already in use
- **THEN** server startup SHALL fail with a clear error message identifying the port conflict; other frontends SHALL continue unaffected

### Requirement: Spark Connect RPC surface
The gRPC server SHALL implement the following `SparkConnectService` methods: `ExecutePlan` (server-streaming), `AnalyzePlan` (unary), `Config` (unary), `Interrupt` (unary), and `ReattachExecute` (server-streaming). The `AddArtifacts` method SHALL return `UNIMPLEMENTED`. All other methods not listed SHALL return `UNIMPLEMENTED`.

#### Scenario: ExecutePlan request routed
- **WHEN** a connected client sends an `ExecutePlan` request with a valid `sessionId` and plan
- **THEN** the server SHALL authenticate the caller, resolve or create the corresponding `SparkConnectSession`, proxy the request to the engine, and stream `ExecutePlanResponse` messages back to the client

#### Scenario: AddArtifacts returns UNIMPLEMENTED
- **WHEN** a client sends an `AddArtifacts` request
- **THEN** the server SHALL return gRPC status `UNIMPLEMENTED` with message "AddArtifacts is not yet supported by Kyuubi Spark Connect frontend"

### Requirement: TLS support
When `kyuubi.frontend.spark.connect.ssl.enabled=true`, the gRPC server SHALL use TLS with the keystore configured by `kyuubi.frontend.spark.connect.ssl.keystore.path` and `kyuubi.frontend.spark.connect.ssl.keystore.password`. The server SHALL refuse plaintext connections when TLS is enabled.

#### Scenario: TLS handshake succeeds
- **WHEN** TLS is configured and a client presents a valid certificate chain
- **THEN** the connection SHALL be established and RPCs proceed normally

#### Scenario: Plaintext rejected when TLS enabled
- **WHEN** TLS is enabled and a client attempts a plaintext (non-TLS) connection
- **THEN** the connection SHALL be rejected before any RPC metadata is read

### Requirement: Configuration keys
The following `KyuubiConf` entries SHALL be added under the `kyuubi.frontend.spark.connect.*` namespace:

| Key | Default | Description |
|---|---|---|
| `kyuubi.frontend.spark.connect.enabled` | `false` | Enable the Spark Connect gRPC frontend |
| `kyuubi.frontend.spark.connect.bind.host` | `<system default>` | Bind hostname/IP |
| `kyuubi.frontend.spark.connect.bind.port` | `15002` | Bind port |
| `kyuubi.frontend.spark.connect.max.message.size` | `128MB` | Maximum inbound message size |
| `kyuubi.frontend.spark.connect.ssl.enabled` | `false` | Enable TLS |
| `kyuubi.frontend.spark.connect.ssl.keystore.path` | (none) | Keystore file path |
| `kyuubi.frontend.spark.connect.ssl.keystore.password` | (none) | Keystore password |

After adding entries, `dev/gen/gen_all_config_docs.sh` SHALL be re-run and the generated diff committed in the same PR.

#### Scenario: Default port
- **WHEN** `kyuubi.frontend.spark.connect.bind.port` is not set
- **THEN** the server SHALL bind to port 15002 (Spark Connect's standard default)

#### Scenario: Missing keystore path with TLS enabled
- **WHEN** `kyuubi.frontend.spark.connect.ssl.enabled=true` and `kyuubi.frontend.spark.connect.ssl.keystore.path` is empty
- **THEN** server startup SHALL fail with a descriptive error indicating the missing keystore configuration
