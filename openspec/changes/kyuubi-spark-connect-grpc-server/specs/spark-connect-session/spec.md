## ADDED Requirements

### Requirement: Session creation on first RPC
A `SparkConnectSession` SHALL be created in Kyuubi's `SessionManager` the first time a client sends any RPC with a `sessionId` that is not already tracked. The session SHALL be keyed by the tuple `(kyuubiUser, sessionId)`. If `sessionId` is absent from the request, Kyuubi SHALL generate a UUID and return it in the first response's `session_id` field.

#### Scenario: New sessionId opens a session
- **WHEN** a client sends `ExecutePlan` with a `sessionId` not present in `SessionManager`
- **THEN** a new `SparkConnectSession` SHALL be created, associated with the authenticated user, and the engine acquisition SHALL begin

#### Scenario: Existing sessionId reuses session
- **WHEN** a client sends a second RPC with the same `sessionId` and same authenticated user
- **THEN** the existing `SparkConnectSession` SHALL be reused; no new engine acquisition occurs

#### Scenario: sessionId collision across users
- **WHEN** two different authenticated users send RPCs with the same `sessionId`
- **THEN** each user SHALL receive an independent session; sessions SHALL be isolated by user identity

### Requirement: Session isolation and resource limits
Each `SparkConnectSession` SHALL be subject to the same per-user engine and session limits enforced by `SessionManager` for Thrift sessions. Specifically:
- `kyuubi.session.engine.share.level` applies (USER, CONNECTION, GROUP, SERVER).
- `kyuubi.session.check.interval` governs idle session cleanup.
- Session-level configs passed via `Config` RPC SHALL be stored in the `SparkConnectSession` and forwarded to the engine at session open time.

#### Scenario: Per-user engine limit honored
- **WHEN** a user already has the maximum allowed engines and opens a new Spark Connect session under `USER` share level
- **THEN** the new session SHALL reuse the existing engine rather than spawning a new one

#### Scenario: Idle session cleanup
- **WHEN** a `SparkConnectSession` has had no RPC activity for longer than `kyuubi.session.idle.timeout`
- **THEN** `SessionManager` SHALL close the session and release the engine connection

### Requirement: Session close
A `SparkConnectSession` SHALL be closed when:
1. The client sends an `Interrupt` RPC with `interrupt_type = SESSION_CLOSE`, OR
2. The session idle timeout elapses, OR
3. Kyuubi server shuts down.

On close, any in-progress `ExecutePlan` or `ReattachExecute` streams to the client SHALL be terminated with gRPC status `CANCELLED` or `UNAVAILABLE` as appropriate.

#### Scenario: Graceful close via Interrupt RPC
- **WHEN** a client sends `Interrupt` with `interrupt_type = SESSION_CLOSE`
- **THEN** the session SHALL be closed, the engine connection released, and the RPC SHALL return success

#### Scenario: Server shutdown terminates sessions
- **WHEN** Kyuubi server initiates graceful shutdown
- **THEN** all active `SparkConnectSession` instances SHALL be closed within the shutdown timeout; streaming clients SHALL receive `UNAVAILABLE`

### Requirement: Event logging
Session open, close, and key lifecycle events for `SparkConnectSession` SHALL be published to Kyuubi's event logging system (same `EventBus` used by Thrift sessions) with event type `SparkConnectSessionEvent`.

#### Scenario: Session open event emitted
- **WHEN** a `SparkConnectSession` is created
- **THEN** an event with `eventType=SESSION_CREATED`, `user`, `sessionId`, and `timestamp` SHALL be published to the event bus
