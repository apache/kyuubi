## ADDED Requirements

### Requirement: Engine-side Spark Connect port discovery
When opening a `SparkConnectSession`, the `SparkConnectEngineProxy` SHALL discover the Spark Connect gRPC port of the assigned engine by reading the engine's service-discovery entry. For ZooKeeper-based discovery, the port SHALL be stored as a node attribute `sparkConnectPort`. For Kubernetes-based discovery, the port SHALL be read from the pod label `kyuubi.apache.org/spark-connect-port` or the pod annotation `kyuubi.apache.org/spark-connect-port`. If the engine's service-discovery entry does not contain a Spark Connect port, session opening SHALL fail with a descriptive error.

#### Scenario: Engine port discovered via ZooKeeper
- **WHEN** the engine registers with a `sparkConnectPort` attribute in its ZooKeeper node
- **THEN** `SparkConnectEngineProxy` SHALL connect to the engine at that port for the session

#### Scenario: Engine not in Spark Connect mode
- **WHEN** the engine's service-discovery entry has no `sparkConnectPort`
- **THEN** session creation SHALL fail with error "Engine does not support Spark Connect; start engine with --spark-connect flag"

### Requirement: Transparent RPC forwarding
For each client RPC (`ExecutePlan`, `AnalyzePlan`, `Config`, `Interrupt`, `ReattachExecute`), the `SparkConnectEngineProxy` SHALL:
1. Open a gRPC channel to the engine (or reuse a cached channel for the session).
2. Forward the request protobuf bytes verbatim to the engine.
3. Stream or return the engine's response protobuf bytes verbatim to the client.
4. Propagate gRPC status codes and trailers from the engine back to the client unchanged.

Kyuubi SHALL NOT deserialize or re-serialize plan proto messages (pass-through at the byte level).

#### Scenario: ExecutePlan streamed end-to-end
- **WHEN** a client sends `ExecutePlan` with a valid plan
- **THEN** each `ExecutePlanResponse` chunk from the engine SHALL be forwarded to the client in order, with no modification, and the stream SHALL close with the same status the engine returns

#### Scenario: Engine returns error status
- **WHEN** the engine returns gRPC status `INTERNAL` for a malformed plan
- **THEN** the client SHALL receive gRPC status `INTERNAL` with the same error detail; Kyuubi SHALL log the failure at WARN level with the `sessionId`

### Requirement: Engine channel lifecycle
The gRPC channel from Kyuubi to the engine SHALL be established at session-open time and closed when the session closes. The channel SHALL be multiplexed across concurrent RPCs within the same session. Channel options SHALL respect `kyuubi.frontend.spark.connect.engine.channel.max.message.size` (default 128MB) and `kyuubi.frontend.spark.connect.engine.channel.keepalive.time`.

#### Scenario: Concurrent RPCs share one channel
- **WHEN** a session has multiple concurrent `Config` and `AnalyzePlan` RPCs in flight
- **THEN** all RPCs SHALL use the same underlying HTTP/2 connection to the engine

#### Scenario: Channel closed on session close
- **WHEN** a `SparkConnectSession` is closed
- **THEN** the engine-side gRPC channel SHALL be shut down gracefully (no abrupt TCP reset unless shutdown timeout exceeded)

### Requirement: Engine connectivity failure handling
If the engine's gRPC channel becomes unavailable after session establishment (network error, engine crash), ongoing streaming RPCs (`ExecutePlan`, `ReattachExecute`) SHALL be terminated with gRPC status `UNAVAILABLE`. Clients MAY reconnect via `ReattachExecute` if the operation is still tracked in Kyuubi's `OperationManager`.

#### Scenario: Engine crashes mid-stream
- **WHEN** the engine process terminates while `ExecutePlan` is streaming
- **THEN** the client SHALL receive `UNAVAILABLE` with message "Engine connection lost"; the session SHALL be marked DEAD in `SessionManager`

#### Scenario: ReattachExecute after transient failure
- **WHEN** a client sends `ReattachExecute` for an operation still tracked in `OperationManager` and the engine has recovered
- **THEN** the proxy SHALL re-establish the engine channel and resume streaming from the last acknowledged response index
