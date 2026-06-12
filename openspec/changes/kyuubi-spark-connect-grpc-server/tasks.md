## 1. Module and Build Infrastructure

- [ ] 1.1 Create `kyuubi-spark-connect/` Maven module with `pom.xml`; add `grpc-netty-shaded`, `protobuf-java`, and `spark-connect-common` (Spark 3.5) as dependencies scoped to `-Pspark-connect` profile
- [ ] 1.2 Add `kyuubi-spark-connect` to the parent `pom.xml` module list under the `-Pspark-connect` profile and to `kyuubi-assembly` packaging
- [ ] 1.3 Run `build/dependency.sh --replace` to update `dev/dependencyList`; verify no license-incompatible transitive deps are pulled in; update `LICENSE-binary` and `NOTICE` as needed
- [ ] 1.4 Configure `grpc-netty-shaded` shading in `kyuubi-spark-connect` to avoid Netty classpath conflicts with the Thrift frontend; validate with `mvn dependency:tree`
- [ ] 1.5 Add Spotless/Scalafmt config for generated proto sources (mark `target/generated-sources` as excluded from style checks)

## 2. Protobuf and gRPC Stubs

- [ ] 2.1 Import `SparkConnectService.proto` and dependent message protos from `spark-connect-common` (Spark 3.5 artifact) into `kyuubi-spark-connect/src/main/protobuf/`
- [ ] 2.2 Configure `protobuf-maven-plugin` to generate Java stubs from proto files into `target/generated-sources/protobuf/java`
- [ ] 2.3 Create `SparkConnectProtoShim` interface to abstract any Spark-version-specific proto field differences; add Spark 3.5 shim implementation
- [ ] 2.4 Verify generated stubs compile cleanly with `build/mvn clean compile -pl kyuubi-spark-connect -Pspark-connect -DskipTests`

## 3. Configuration Entries

- [ ] 3.1 Add `KyuubiConf` entries for `kyuubi.frontend.spark.connect.enabled`, `bind.host`, `bind.port` (default 15002), `max.message.size`, `ssl.enabled`, `ssl.keystore.path`, `ssl.keystore.password` in `kyuubi-common`
- [ ] 3.2 Add engine-channel config entries: `kyuubi.frontend.spark.connect.engine.channel.max.message.size`, `kyuubi.frontend.spark.connect.engine.channel.keepalive.time`
- [ ] 3.3 Mark server-only entries with `.serverOnly` on the `ConfigEntry` builder so they are stripped before engine propagation
- [ ] 3.4 Run `dev/gen/gen_all_config_docs.sh` and commit the regenerated `docs/configuration/settings.md` diff

## 4. Session and Operation Types

- [ ] 4.1 Create `SparkConnectSession` class extending Kyuubi's `AbstractSession`; keyed by `(user, sessionId)`; stores session-level config map from `Config` RPCs
- [ ] 4.2 Create `SparkConnectOperation` class extending `AbstractOperation`; holds the engine-side gRPC call reference and last-acknowledged response index for `ReattachExecute`
- [ ] 4.3 Extend `SessionManager.openSession` to accept a `SparkConnectSession` type and apply existing per-user engine limits and idle-timeout logic
- [ ] 4.4 Define `SparkConnectSessionEvent` and wire it into Kyuubi's `EventBus` for session open/close events

## 5. Engine Proxy

- [ ] 5.1 Create `SparkConnectEngineProxy` class; implement engine gRPC channel creation using `ManagedChannelBuilder` with configurable max message size and keepalive
- [ ] 5.2 Extend ZooKeeper-based `EngineRef` to store and retrieve `sparkConnectPort`; update `AbstractEngineRef.doRegister` to write this attribute when engine starts in `--spark-connect` mode
- [ ] 5.3 Extend Kubernetes-based engine discovery to read `kyuubi.apache.org/spark-connect-port` label/annotation from engine pod and expose it via `EngineRef`
- [ ] 5.4 Implement `ExecutePlan` forwarding: open server-streaming call to engine, pipe `ExecutePlanResponse` chunks back to client `StreamObserver`, propagate gRPC status and trailers
- [ ] 5.5 Implement `AnalyzePlan`, `Config`, and `Interrupt` forwarding (unary RPCs — simpler than streaming)
- [ ] 5.6 Implement `ReattachExecute` forwarding: look up tracked `SparkConnectOperation` in `OperationManager`, re-establish engine stream from last acknowledged index
- [ ] 5.7 Implement engine channel close on session close; handle in-flight stream termination with `UNAVAILABLE` status to client
- [ ] 5.8 Override `user_name` in forwarded `UserContext` with the server-authenticated Kyuubi user identity before forwarding any request to the engine

## 6. Authentication Interceptor

- [ ] 6.1 Create `SparkConnectAuthInterceptor` implementing `io.grpc.ServerInterceptor`; extract `Authorization: Bearer <token>` from gRPC metadata
- [ ] 6.2 Validate the token via Kyuubi's `AuthenticationProvider` chain; attach resolved `KyuubiUser` to gRPC `Context`
- [ ] 6.3 Return `UNAUTHENTICATED` for missing or invalid tokens; log failures at WARN level with client IP and timestamp
- [ ] 6.4 Implement `NONE` auth bypass: when `kyuubi.authentication=NONE`, skip token validation and attribute to anonymous user (consistent with Thrift frontend)
- [ ] 6.5 Add authorization check in `SparkConnectFrontendService` before forwarding `ExecutePlan` / `AnalyzePlan` to engine; return `PERMISSION_DENIED` on denial

## 7. Frontend Service and Server

- [ ] 7.1 Create `SparkConnectFrontendService` implementing `SparkConnectServiceGrpc.SparkConnectServiceImplBase`; route each RPC method to `SparkConnectSession` / `SparkConnectEngineProxy`
- [ ] 7.2 Return `UNIMPLEMENTED` for `AddArtifacts` with message "AddArtifacts is not yet supported by Kyuubi Spark Connect frontend"
- [ ] 7.3 Create `SparkConnectFrontend` class (mirrors `ThriftFrontendService` pattern) wrapping `io.grpc.Server`; implement `initialize(conf)`, `start()`, `stop()` lifecycle methods
- [ ] 7.4 Configure TLS in `SparkConnectFrontend` when `kyuubi.frontend.spark.connect.ssl.enabled=true`; validate keystore path at startup and fail fast if missing
- [ ] 7.5 Wire `SparkConnectFrontend` into `KyuubiServer.initialize` / `start` / `stop` behind the `kyuubi.frontend.spark.connect.enabled` guard

## 8. Engine-Side Registration (Spark Engine Module)

- [ ] 8.1 In `kyuubi-spark-sql-engine`, detect when `--spark-connect` mode is active and read the embedded gRPC server's bound port at engine startup
- [ ] 8.2 Write `sparkConnectPort` to the engine's ZooKeeper `EngineRef` node alongside the existing Thrift URL attribute
- [ ] 8.3 Add `kyuubi.apache.org/spark-connect-port` label to the engine pod spec in the Kubernetes engine launcher when `--spark-connect` mode is active

## 9. Tests

- [ ] 9.1 Unit test `SparkConnectAuthInterceptor`: valid token, missing token, invalid token, NONE-mode bypass
- [ ] 9.2 Unit test `SparkConnectEngineProxy` port-discovery logic: ZooKeeper node with `sparkConnectPort`, node without `sparkConnectPort` (expect error)
- [ ] 9.3 Unit test `SparkConnectSession` lifecycle: create, reuse by sessionId, user isolation (same sessionId, different users = different sessions), idle-timeout close
- [ ] 9.4 Integration test `SparkConnectFrontendSuite`: start Kyuubi with a mock engine gRPC server; send `ExecutePlan`, `AnalyzePlan`, `Config`, `Interrupt` requests and verify forwarding
- [ ] 9.5 Integration test: `AddArtifacts` returns `UNIMPLEMENTED`
- [ ] 9.6 Integration test: TLS handshake success and plaintext rejection when TLS is enabled
- [ ] 9.7 Integration test: `user_name` in forwarded `UserContext` is overridden with server-authenticated user

## 10. Documentation and Release Prep

- [ ] 10.1 Add user-facing documentation page `docs/connector/spark/spark_connect.md` covering: feature overview, prerequisites (Spark 3.5+), configuration keys, PySpark client example (`spark.remote("sc://...")`), known limitations (no `AddArtifacts`)
- [ ] 10.2 Update `docs/deployment/kyuubi_on_kubernetes.md` and `docs/deployment/on_yarn.md` to note Spark Connect port exposure requirements
- [ ] 10.3 Run `dev/reformat` and confirm CI style checks pass before opening PR
- [ ] 10.4 Fill PR template: link GitHub issue, describe motivation, list test commands, note "Assisted-by: Claude:claude-sonnet-4-6" in the generative AI field
