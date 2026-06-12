## ADDED Requirements

### Requirement: Token-based authentication via gRPC metadata
All incoming gRPC RPCs to the Spark Connect frontend SHALL pass through an `AuthInterceptor` before reaching `SparkConnectFrontendService`. The interceptor SHALL extract the bearer token from the `Authorization` gRPC metadata header (format: `Bearer <token>`) and validate it using Kyuubi's existing `AuthenticationProvider` chain. The resolved `KyuubiUser` SHALL be attached to the gRPC `Context` for use in session and engine routing.

#### Scenario: Valid bearer token accepted
- **WHEN** a client sends any RPC with a valid `Authorization: Bearer <token>` header
- **THEN** the interceptor SHALL resolve the token to a `KyuubiUser` and proceed to the handler

#### Scenario: Missing authorization header
- **WHEN** a client sends any RPC without an `Authorization` metadata header
- **THEN** the interceptor SHALL return gRPC status `UNAUTHENTICATED` with message "Missing Authorization header" before routing the RPC

#### Scenario: Invalid or expired token
- **WHEN** a client sends any RPC with a token that fails validation (expired, malformed, unknown)
- **THEN** the interceptor SHALL return gRPC status `UNAUTHENTICATED` with a non-revealing error message; the failure SHALL be logged at WARN level with client IP and timestamp

### Requirement: User identity propagation
The authenticated `KyuubiUser` identity resolved by the `AuthInterceptor` SHALL be used for:
- Keying the `SparkConnectSession` (user isolation).
- Engine acquisition via `SessionManager` (user-level share level and limits).
- Audit event emission (session open/close events carry the user identity).
- SQL authorization checks when the engine performs authorization (user identity passed as `UserContext.user_name` in forwarded RPCs, overriding any client-supplied value).

#### Scenario: Client-supplied user_name overridden
- **WHEN** a client sends `ExecutePlan` with `UserContext.user_name` set to an arbitrary value
- **THEN** Kyuubi SHALL replace `user_name` in the forwarded `UserContext` with the server-authenticated identity; the client-supplied value SHALL be ignored

#### Scenario: User identity in audit log
- **WHEN** a session is opened by authenticated user `alice`
- **THEN** the session open event SHALL record `user=alice` in the event log, regardless of any value in the client's `UserContext`

### Requirement: Anonymous access disabled
When `kyuubi.authentication` is set to any value other than `NONE`, unauthenticated Spark Connect connections SHALL be rejected. When `kyuubi.authentication=NONE`, all connections SHALL be accepted and attributed to an anonymous user (consistent with Thrift frontend behavior in NONE mode).

#### Scenario: Auth mode NONE allows all connections
- **WHEN** `kyuubi.authentication=NONE` and a client sends an RPC without an `Authorization` header
- **THEN** the RPC SHALL be accepted and attributed to the anonymous user identity

#### Scenario: Auth mode LDAP rejects unauthenticated
- **WHEN** `kyuubi.authentication=LDAP` and a client sends an RPC without an `Authorization` header
- **THEN** the interceptor SHALL return `UNAUTHENTICATED`

### Requirement: Authorization checks at operation level
Before routing `ExecutePlan` or `AnalyzePlan` to the engine, Kyuubi SHALL invoke the configured `AuthorizationProvider` (e.g., Ranger) with the authenticated user identity. If authorization is denied, Kyuubi SHALL return gRPC status `PERMISSION_DENIED` without forwarding the request to the engine.

#### Scenario: Authorized plan executed
- **WHEN** the `AuthorizationProvider` grants access for the authenticated user and the requested operation
- **THEN** the request SHALL be forwarded to the engine normally

#### Scenario: Unauthorized plan rejected
- **WHEN** the `AuthorizationProvider` denies access
- **THEN** gRPC status `PERMISSION_DENIED` SHALL be returned to the client; no request SHALL be forwarded to the engine; the denial SHALL be logged at INFO level
