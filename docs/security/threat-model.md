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

# Kyuubi Security Threat Model

This document defines the security boundaries that Kyuubi must preserve. Use
this document for the first review of a security report. This document is not a
deployment guide. A maintainer makes the final security decision.

## Purpose

Apache Kyuubi is a multi-tenant SQL gateway. It authenticates clients. It
creates sessions and operations. It starts or reuses SQL engines. These engines
connect to cluster managers, metadata services, storage systems, and external
data sources. Review each report against its deployment profile and identity
boundary.

This document has two goals:

1. Give maintainers a shared model of Kyuubi's assets, trust boundaries, and
   security invariants.
2. Make the first assessment of a report consistent and evidence-based.

## Security Goals

Use these security goals during triage.

| ID |            Goal             |                                                                                                       Meaning                                                                                                        |
|----|-----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| G1 | Authentication integrity    | Kyuubi must reject attempts to impersonate another principal or bypass authentication.                                                                                                                               |
| G2 | Proxy identity integrity    | A user must not select an unauthorized session user or proxy user. An administrator can authorize impersonation.                                                                                                     |
| G3 | Tenant isolation            | A tenant must not read or change another tenant's sessions, operations, results, logs, engines, credentials, or data. An explicit deployment policy can grant access.                                                |
| G4 | Authorization preservation  | Kyuubi must pass the authenticated identity to the engine and data platform. It must not weaken the configured authorization policy.                                                                                 |
| G5 | Secret confidentiality      | Kyuubi must not expose secrets to unauthorized tenants, logs, responses, or uploaded resources. Secrets include passwords, tokens, keytabs, ticket caches, keystores, internal secrets, and data-source credentials. |
| G6 | Control-plane authorization | Only an administrator can do administrator actions. Only an owner or administrator can manage tenant-owned resources. Engine actions must follow the configured owner and administrator policy.                      |
| G7 | Data integrity              | A tenant must not change another tenant's query, engine, metadata, or result without authorization.                                                                                                                  |
| G8 | Availability and fairness   | A bounded request from an unauthenticated attacker or low-privilege tenant must not crash Kyuubi, bypass resource limits, starve other tenants, or stop their work.                                                  |
| G9 | Audit usefulness            | Security events must contain enough identity and event data for an investigation. They must not contain secrets.                                                                                                     |

Kyuubi does not replace authorization in Hadoop, Spark, Ranger, metastores,
databases, or object stores. Determine whether Kyuubi violates G4. Do not report
behavior that an external system permits by policy as a Kyuubi vulnerability.

## Scope

### In scope

- Kyuubi server frontends and protocol handling, including Thrift binary,
  Thrift HTTP, REST, the REST web UI, direct and proxied engine UIs, the Trino
  HTTP frontend, and every configured metrics reporter.
- Authentication and identity propagation for Kerberos, LDAP, JDBC, custom
  basic or bearer providers, and unauthenticated development modes.
- Session, operation, batch, result, log, and engine lifecycle handling.
- Proxy-user and impersonation checks, engine sharing, service discovery, and
  server-to-engine communication.
- Kyuubi interactions with YARN, Kubernetes, ZooKeeper, etcd, Hadoop
  delegation-token services, Hive Metastore, Ranger, JDBC data sources, and
  storage systems when Kyuubi makes or transforms a security decision.
- Credentials and sensitive configuration handled by Kyuubi, including
  Kerberos keytabs and ticket caches, delegation tokens, LDAP and JDBC
  credentials, TLS material, internal engine secrets, object-store
  credentials, and Kubernetes service-account credentials.
- The Data Agent engine, including its model provider, prompts and conversation
  history, JDBC data source, SQL tools, approval mode, and web UI integration.
- Official Helm templates, container configuration, CI workflows, release
  artifacts, and dependencies when they change Kyuubi's security posture.

### Out of scope

- A vulnerability in an external service when Kyuubi does not cause or increase
  the effect. Send such a report to the upstream project or platform owner.
- Compromise of a Kyuubi administrator, Kubernetes cluster administrator,
  Hadoop administrator, KDC, LDAP directory, Ranger administrator, or storage
  administrator. Kyuubi must still avoid exposing their credentials to a
  tenant or logging them accidentally.
- Volumetric network DDoS, link saturation, or an availability failure of an
  external service. Application-level exhaustion caused by a Kyuubi request is
  in scope under G8.
- Data access, SQL execution, UDFs, extensions, or uploaded application code
  that the deployment deliberately authorizes for that tenant. Escape from the
  assigned identity, container, engine, or data policy remains in scope.
- An operator's choice to expose an intentionally insecure development mode,
  such as `NONE`, `NOSASL`, or plaintext HTTP, when the product clearly
  documents the consequence. An insecure default, unsafe chart default, or
  misleading security documentation is still a potential product issue.
- The experimental MySQL frontend in releases that include it. This frontend is
  not ready for production use. A MySQL-only finding is not a vulnerability
  under this model. Report it as a normal bug. Kyuubi removed this frontend in
  1.12.0.

## Deployment Profiles

Identify the deployment profile before you assess a report. Do not make a
conclusion without this information.

### Shared secure deployment

This is the primary security profile for multi-tenant use:

- Client authentication is enabled. Configure transport confidentiality and
  integrity separately. Authentication does not encrypt the transport. Record
  the TLS settings and the SASL quality of protection.
  Native TLS settings cover the Thrift binary and Thrift HTTP frontends. REST
  and Trino need an external TLS terminator or an isolated trusted network.
- Kyuubi preserves the effective identity during session creation and engine
  submission. Alternatively, the deployment defines and authorizes an engine
  principal, such as a server principal or a group principal.
- Authorization is configured in the engine and data platform, for example
  through Hadoop permissions or the Kyuubi Spark authorization plugin.
- Engine, log, temporary-file, service-discovery, and cluster-manager access
  is isolated according to the deployment's tenant model.
- Kubernetes or YARN privileges are limited to what Kyuubi and its engines
  require.
- Internal server-engine and server-server authentication is enabled when
  those channels cross an untrusted boundary. ZooKeeper or etcd authentication,
  transport protection, and ACLs are configured independently.

### Isolated development or trusted-user deployment

Kyuubi supports modes that do not authenticate clients or encrypt the
transport. Use these modes only on an isolated development network. These modes
do not provide a security boundary between tenants. The use of `NONE`,
`NOSASL`, or plaintext HTTP is not proof of an authentication bypass. However,
do not reject a report only as configuration hardening until the project
defines this policy boundary. Treat the behavior as a product issue if an
official artifact enables the insecure mode without a clear warning. Also
treat it as a product issue if Kyuubi describes the mode as secure.

### Kubernetes or YARN deployment

The deployment boundary includes the cluster manager, its API, service
accounts, network policy, image registry, and storage permissions. Kyuubi must
submit the correct identity and privilege values. Kyuubi must not request or
expose more access than it needs.

## System Context

```mermaid
flowchart LR
    C[Client or tenant] -->|Thrift binary / Thrift HTTP / REST| K[Kyuubi server]
    K -->|Kerberos / LDAP / JDBC / custom provider| I[Identity provider]
    K -->|Registration and HA state| H[ZooKeeper or etcd]
    K -->|Internal channel; profile-dependent auth| E[SQL engine]
    K -->|Submit, inspect, stop| M[YARN or Kubernetes]
    E -->|Identity-aware query and credentials| D[Storage, metastore, databases]
    E -->|Policy checks and audit| R[Ranger or engine authorization]
    E -->|Data Agent prompt and history| P[Configured model provider]
    E -->|Data Agent JDBC tools| J[Configured data source]
    C -->|Direct engine UI when enabled| E
    K -->|Logs, metrics, temporary files| L[Runtime filesystem and observability]
    O[Operator configuration and secrets] --> K
```

Important flows are:

1. A client authenticates to a Kyuubi frontend. Kyuubi records the real user.
   Kyuubi creates a session user. The session user can be an authorized proxy
   user.
2. A session submits SQL or a batch request. Kyuubi selects, creates, or
   reuses an engine according to the configured sharing level.
3. Kyuubi submits an engine to a cluster manager and passes the identity,
   session configuration, and required credentials.
4. The engine queries storage and metadata services and returns results,
   operation logs, and status through Kyuubi.
5. A Data Agent engine may send conversation context to its configured model
   provider. It can run approved SQL tools against its configured data source.
6. Kyuubi instances use service discovery and metadata storage to share
   reachability and recover sessions or batches in high-availability mode.

The architecture description is in
[`docs/overview/architecture.md`](../overview/architecture.md). The REST
surface is documented in [`docs/client/rest/rest_api.md`](../client/rest/rest_api.md).

## Trust Boundaries

| ID  |                    Boundary                     |                                 Trust change                                 |                                                  Main question for a report                                                   |
|-----|-------------------------------------------------|------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| TB1 | Client to frontend                              | Network input becomes a Kyuubi request                                       | Can an attacker reach a frontend and authenticate or submit a request they should not be able to submit?                      |
| TB2 | Frontend to identity provider                   | A claimed identity becomes an authenticated principal                        | Can the request bypass, confuse, or downgrade the configured authentication mechanism?                                        |
| TB3 | Real user to session/proxy user                 | One principal is mapped to the identity used by the engine                   | Can a tenant select another user or bypass the proxy-user policy?                                                             |
| TB4 | Kyuubi server to engine                         | Server-controlled state and credentials enter an engine process              | Can an engine or tenant alter the identity, obtain another tenant's state, or use internal credentials outside its grant?     |
| TB5 | Kyuubi to cluster manager and HA store          | Kyuubi obtains control-plane capabilities                                    | Can a request create, inspect, delete, or reconfigure resources outside its tenant and administrator scope?                   |
| TB6 | Engine to data platform                         | Query identity reaches data and metadata services                            | Does Kyuubi preserve the configured authorization decision and tenant identity?                                               |
| TB7 | Operator configuration and artifacts to runtime | Files, images, dependencies, and secrets become executable behavior          | Does an official artifact or Kyuubi code expose secrets, grant excess privilege, or change the security profile unexpectedly? |
| TB8 | Data Agent and model or data-source provider    | Prompts, query results, credentials, and tool calls leave the engine process | Can a model or data-source interaction disclose data, change a data source, or run a tool outside the approved tenant scope?  |

Use a client IP value from a proxy header only for logs and policy checks. Do
not use this value as proof of identity. Kyuubi configuration has separate
settings for the remote address and the forwarded header.

## Assets

|                                                    Asset                                                     | Confidentiality | Integrity | Availability |
|--------------------------------------------------------------------------------------------------------------|-----------------|-----------|--------------|
| Query results and source data                                                                                | High            | High      | High         |
| Session, operation, batch, and engine metadata                                                               | Medium to high  | High      | High         |
| Query and engine logs                                                                                        | Medium to high  | Medium    | Medium       |
| User, real-user, and proxy-user identity                                                                     | High            | High      | High         |
| Kerberos keytabs, TGTs, ticket caches, and delegation tokens                                                 | Critical        | High      | High         |
| LDAP bind passwords, JDBC authentication credentials, bearer tokens, TLS keys, and internal security secrets | Critical        | High      | High         |
| Kubernetes service-account tokens, kubeconfig files, YARN credentials, and object-store keys                 | Critical        | High      | High         |
| Data Agent model API keys, JDBC URLs, conversation history, prompts, tool calls, and tool results            | Critical        | High      | High         |
| Service-discovery registrations and HA metadata                                                              | Medium          | High      | High         |
| Cluster-manager applications, pods, and resource queues                                                      | Medium          | High      | High         |
| Metrics and diagnostic endpoint data                                                                         | Medium          | Medium    | Medium       |
| Configuration and official release artifacts                                                                 | Medium          | Critical  | High         |
| Worker threads, engine slots, cluster resources, and storage capacity                                        | Low to medium   | Medium    | High         |

## Adversaries

| ID |                      Adversary                      |                                                   Capabilities                                                    |                                                Typical goal                                                |
|----|-----------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| A1 | Unauthenticated network attacker                    | Can send requests to an exposed frontend but has no valid identity                                                | Bypass authentication, invoke an administrative action, read data, or exhaust Kyuubi.                      |
| A2 | Malicious authenticated tenant                      | Has a valid low-privilege Kyuubi identity and can submit SQL, session configuration, and permitted batch requests | Read or alter another tenant's data, impersonate a user, reach another engine, or starve shared resources. |
| A3 | Malicious tenant artifact or engine                 | Can supply an authorized UDF, JAR, batch resource, connector, or data-source configuration                        | Escape the intended engine or tenant boundary, steal server credentials, or affect another tenant.         |
| A4 | Compromised dependency, image, plugin, or CI action | Can execute code in a build or runtime process at that process's privilege                                        | Steal credentials, alter release artifacts, or add a backdoor to a distributed image or package.           |
| A5 | Network or integration attacker                     | Can tamper with an unprotected or incorrectly verified connection to an identity, HA, cluster, or data service    | Feed false identity, policy, engine, or metadata information to Kyuubi.                                    |
| A6 | Curious or negligent privileged operator            | Has legitimate access to configuration, secrets, logs, or cluster controls                                        | Accidentally expose secrets or create a deployment that violates the stated tenant boundary.               |
| A7 | Malicious data, prompt, or model provider           | Can influence data returned to a Data Agent, the model endpoint, or a model-generated tool request                | Cause unauthorized SQL, data-source access, data disclosure, or cross-session history exposure.            |

A6 does not define an administrator as an external attacker. An operator
controls the deployment. Kyuubi must still fail safely, redact secrets, and use
least-privilege defaults.

## STRIDE Threat Register

Use this register as a triage aid. A matching row does not prove that an
implementation is vulnerable. Verify the code path and deployment profile.

| ID  |                     STRIDE                     |                                                                                                         Threat                                                                                                         |      Boundary      |                                                                                                   Reportable when                                                                                                   |
|-----|------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| T01 | Spoofing                                       | A client can authenticate as another user, or a frontend accepts a caller-supplied identity such as a Trino request-user header without an independent authentication boundary.                                        | TB1, TB2           | The behavior occurs with authentication enabled, or the frontend is exposed in a profile that treats the caller as authenticated without verifying the identity.                                                    |
| T02 | Spoofing / Elevation                           | A tenant can set `hive.server2.proxy.user` or `kyuubi.session.proxy.user` to an unauthorized user.                                                                                                                     | TB3                | The Hadoop proxy-user or equivalent authorization check is bypassed, confused, or applied to the wrong real user or address.                                                                                        |
| T03 | Tampering                                      | Session configuration changes a server-only security setting or changes the identity, engine share level, or authorization context after validation.                                                                   | TB3, TB4           | A tenant can override an immutable or server-only setting and cross a security boundary.                                                                                                                            |
| T04 | Spoofing / Tampering                           | A forged engine registration, internal token, service-discovery record, or HA message is accepted as a trusted engine or server.                                                                                       | TB4, TB5           | An attacker can register, impersonate, redirect, replay, or tamper with a peer because internal-token integrity, token scope, replay handling, HA-store authentication, transport, or ACLs are missing or bypassed. |
| T05 | Information disclosure                         | A tenant can read another tenant's result, operation log, session event, engine endpoint, engine metadata, or batch resource.                                                                                          | TB3, TB4, TB6      | No explicit policy grants access to the artifact. A shared engine does not grant access to another session's results or logs.                                                                                       |
| T06 | Information disclosure                         | A keytab, ticket cache, delegation token, password, bearer token, TLS private key, internal secret, object-store credential, or Data Agent credential is returned, logged, mounted, or left in a tenant-readable path. | TB4, TB7, TB8      | The secret is exposed beyond the principal or process that needs it. Check local-path allow lists, redaction patterns, configuration response mode, logs, and browser or session history.                           |
| T07 | Information disclosure                         | A REST, Thrift, Trino, UI, metrics, error, or diagnostic response reveals confidential data, credentials, or security-sensitive configuration to an unauthorized caller.                                               | TB1, TB3, TB7, TB8 | The response is reachable in the relevant profile and the data is not intentionally public operational metadata.                                                                                                    |
| T08 | Tampering / Elevation                          | A caller performs an administrator-only action or reads, deletes, refreshes, stops, or reconfigures a resource it does not own.                                                                                        | TB1, TB3, TB5      | The authorization check is absent, uses the wrong identity, authorizes a bearer handle alone, or can be bypassed. An owner-scoped engine action is not a violation.                                                 |
| T09 | Elevation                                      | Kyuubi requests or uses more YARN, Kubernetes, ZooKeeper, etcd, file-system, or data-source privileges than it needs.                                                                                                  | TB5, TB6, TB7      | An official default, required code path, or official guide grants the excessive privilege. An operator-selected override alone does not qualify.                                                                    |
| T10 | Tampering / Information disclosure             | Kyuubi passes a user-controlled configuration, URL, path, class, connector, resource, or Data Agent data source to a backend. The input changes the target or bypasses authorization.                                  | TB3, TB5, TB6, TB8 | The input permits SSRF, path traversal, credential substitution, unauthorized data-source access, or policy bypass under a supported profile.                                                                       |
| T11 | Elevation                                      | An engine, plugin, uploaded batch resource, connector, or Data Agent tool can escape its intended tenant or process boundary through Kyuubi-owned behavior.                                                            | TB4, TB5, TB7, TB8 | The escape depends on a Kyuubi flaw, unsafe official artifact, or Kyuubi-granted privilege, not merely on code the operator intentionally trusts.                                                                   |
| T12 | Denial of service                              | A bounded unauthenticated or low-privilege request crashes Kyuubi, exhausts frontend workers, creates unbounded engines, uploads unbounded resources, bypasses resource limits, or starves another tenant.             | TB1, TB3, TB5, TB8 | The impact crosses tenants or privilege levels and is not simply the expected cost of a query within an operator-defined quota.                                                                                     |
| T13 | Denial of service                              | Credential renewal, engine recovery, service discovery, or failover can be driven into an unsafe loop or can disable unrelated tenants.                                                                                | TB4, TB5, TB7      | A tenant or network attacker can trigger the condition through a supported request or protocol path.                                                                                                                |
| T14 | Repudiation                                    | Security-relevant actions lose the identity or event information needed to determine who submitted, stopped, or altered a request.                                                                                     | TB1, TB3, TB5      | The loss prevents meaningful investigation and is caused by Kyuubi's handling, not by an operator choosing not to retain logs.                                                                                      |
| T15 | Supply-chain tampering                         | A dependency, plugin, image, release artifact, or CI workflow can be substituted or altered without the expected verification boundary.                                                                                | TB7                | The weakness is in Kyuubi-owned build, release, chart, or dependency handling and can affect distributed users.                                                                                                     |
| T16 | Tampering / Information disclosure / Elevation | Data Agent prompt or data-source content changes a model-generated tool call. Model output or provider output exposes another tenant's history, data, credentials, or tool result.                                     | TB4, TB6, TB8      | The behavior crosses a data-source, session, approval, or tenant boundary. Prompt injection without a security effect does not qualify.                                                                             |

## First-Pass Vulnerability Triage

### Required report facts

Get these facts before you decide that a report is invalid:

- Kyuubi version, deployment mode, engine type, and relevant commit or image
  digest.
- Whether the frontend is Thrift binary, Thrift HTTP, REST, Trino, metrics, or
  the web UI.
- Authentication method, SASL quality of protection, TLS, and transport
  protection actually enabled.
- For Thrift HTTP: cookie authentication, `doAs` proxy identity, and XSRF
  filter settings.
- Whether an engine UI is opened directly or through the REST engine-UI proxy,
  and the independent access controls on the target engine UI.
- Every enabled metrics reporter: Prometheus, JMX, JSON, console, or SLF4J.
- Attacker identity: unauthenticated, tenant, administrator, engine process,
  cluster principal, or network attacker.
- Exact preconditions, request, SQL, session configuration, uploaded resource,
  or network position.
- Real user and target session/proxy user, if impersonation is involved.
- Target asset and impact: data, identity, secret, integrity, control-plane
  privilege, or availability.
- Reproduction steps, expected result, observed result, and whether a second
  tenant or administrator is affected.
- Relevant Hadoop, Kubernetes, Ranger, LDAP, Kerberos, storage, or database
  policy configuration.
- Internal security settings and ZooKeeper or etcd authentication, transport,
  namespace, and ACL configuration.
- Resource limits, engine-startup limits, upload limits, temporary-directory
  policy, redaction settings, and configuration response mode.
- For Data Agent reports: model endpoint, API-key handling, data-source URL and
  credentials, share level, approval mode, session history, and tool call.

A reporter can provide a severity or CVSS value. Treat this value as part of the
claim. It is not the Kyuubi severity decision.

### Decision procedure

Do these steps in order. Do not make a decision from a keyword such as
"unauthenticated", "SQL", "Kubernetes", or "DoS".

1. **Normalize the claim.** Rewrite the report as: "An attacker with [principal]
   can use [entry point and input] under [preconditions] to affect [asset],
   crossing [boundary]."
2. **Confirm the product path.** Identify the relevant Kyuubi code, chart,
   image, dependency, Data Agent path, or documented integration. If only an
   external platform causes the behavior, send the report to that platform.
3. **Identify the deployment profile.** Record authentication, TLS/SASL,
   engine sharing, authorization plugin, cluster manager, internal security,
   HA-store auth/ACL, and relevant RBAC or proxy-user settings. Missing profile
   information produces `NEEDS-EVIDENCE`, not `INVALID`.
4. **Identify the attacker and required privilege.** A cluster administrator or
   Kyuubi administrator is not an ordinary tenant. Treat a tenant-supplied
   engine artifact, plugin, JAR, or data source as untrusted input unless the
   deployment explicitly trusts it. Do not reject a report only because it uses
   an artifact.
5. **Test a security goal.** Map the observed behavior to G1-G9 and a threat
   row T01-T16. If no goal or boundary is implicated, it is likely expected
   behavior or a normal bug rather than a security report.
6. **Check for authorized behavior.** The following actions are not
   vulnerabilities by themselves: permitted SQL, access to the tenant's own
   results and logs, use of an explicitly shared engine, and access that a
   backend policy permits. Engine sharing does not permit access to another
   session's artifacts. The data-source policy and tenant policy apply to every
   Data Agent query.
7. **Assess the effect.** The following effects can be valid vulnerabilities:
   cross-tenant data access, authentication bypass, unauthorized impersonation,
   credential exposure, control-plane elevation, and denial of service across
   tenants. Volumetric DDoS is outside this model. An expensive but authorized
   query is also outside this model unless Kyuubi bypasses a documented limit.
8. **Check reproduction and reachability.** Confirm that a supported version
   and profile can reach the path. Use `NEEDS-EVIDENCE` for a theoretical
   concern. Do not declare it fixed or invalid from static analysis alone. Use
   `INVALID-OR-UNREPRODUCIBLE` only after a reasonable reproduction attempt with
   all relevant facts.
9. **Record the assessment.** Document the disposition, confidence, reasoning,
   and evidence requests. A maintainer decides whether to open, close, merge,
   or escalate the report.

### Dispositions

|         Disposition         |                                                                                 Use when                                                                                  |                                                Next action                                                 |
|-----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| `VALID-CANDIDATE`           | There is a plausible, reachable violation of G1-G9 under a supported secure profile.                                                                                      | Preserve the report, reproduce, and begin maintainer security triage.                                      |
| `NEEDS-EVIDENCE`            | The claim could cross a boundary, but version, profile, identity, or reproduction facts are missing.                                                                      | Ask focused questions; do not reject the report.                                                           |
| `NEEDS-HUMAN-REVIEW`        | The report requires maintainer judgment before a disposition or action.                                                                                                   | Route it to a maintainer for review.                                                                       |
| `EXPECTED-BEHAVIOR`         | The behavior is an explicitly authorized tenant or administrator capability and no security goal is violated.                                                             | Explain the boundary and link the relevant documentation.                                                  |
| `CONFIGURATION-HARDENING`   | The behavior requires an operator-selected insecure mode or excessive external privilege, without a Kyuubi defect in the supported secure profile.                        | Recommend configuration or documentation improvement; reassess if the default or official chart is unsafe. |
| `DEPENDENCY-OR-PLATFORM`    | The root cause is entirely in an external dependency or platform and Kyuubi neither introduces nor worsens the security impact.                                           | Route to the dependency or platform owner and record the Kyuubi integration context.                       |
| `INVALID-OR-UNREPRODUCIBLE` | Complete relevant facts and a reasonable reproduction attempt show that the claim has no reachable path or plausible security impact. Missing facts alone do not qualify. | Keep the evidence and provide a concise technical explanation.                                             |

Use `NEEDS-EVIDENCE` when facts are missing from a report about data access,
impersonation, credential exposure, or availability across tenants. Do not
close a report only because confidence is low.

## Common Report Patterns

|                               Report pattern                                |                                                                                                                                                              Initial assessment                                                                                                                                                               |
|-----------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| "Kyuubi accepts a connection without a password."                           | Inspect the authentication method and transport. `NONE`, `NOSASL`, and plaintext HTTP are supported modes. Exposure in a shared secure profile can be a vulnerability. An unsafe official default can also be a vulnerability. Do not reject the report only as configuration hardening until the project defines the policy for these modes. |
| "A user can run arbitrary SQL."                                             | Expected for a SQL gateway unless the SQL escapes the tenant's configured data and engine authorization. Ask which principal the backend sees and whether another tenant's data is reachable.                                                                                                                                                 |
| "A user can set `hive.server2.proxy.user` to another user."                 | Potentially valid. Verify the real user, source address, Hadoop proxy-user policy, administrator status, and whether Kyuubi's check is bypassed.                                                                                                                                                                                              |
| "A user can read another user's operation log, result, session, or engine." | `VALID-CANDIDATE` unless an explicit shared-engine or data policy grants the access. This directly tests G3 and usually requires preservation of the original report.                                                                                                                                                                         |
| "A Trino client sets `X-Trino-User`."                                       | Treat the header as an identity claim, not authentication. Verify the independent authentication and network boundary of the Trino frontend before accepting the claimed user.                                                                                                                                                                |
| "A query consumes a lot of resources."                                      | A candidate only when an unauthenticated or low-privilege tenant can bypass a documented limit, crash the service, or starve another tenant. A costly query within the configured resource policy is normally availability governance.                                                                                                        |
| "The Kubernetes service account can create pods."                           | Compare the action with the role in the official chart. An operator-supplied broad role is configuration hardening. An official default or guide can be a vulnerability if it grants access to unrelated namespaces or secrets.                                                                                                               |
| "A keytab or token appears in a local directory or log."                    | Potentially valid if a tenant, engine, diagnostic endpoint, or log reader can obtain it. The operator's private keytab storage is not itself a Kyuubi vulnerability, but Kyuubi must not make it tenant-readable.                                                                                                                             |
| "The Data Agent sends query data to a model or permits a change."           | Verify the provider endpoint, data-source identity, session boundary, approval mode, and tool risk. Treat model and data-source content as untrusted input. Prompt injection is reportable only when it causes an unauthorized security effect.                                                                                               |
| "A vulnerability affects the MySQL frontend."                               | The experimental MySQL frontend is outside this model. Report a MySQL-only finding as a normal bug. Kyuubi removed this frontend in 1.12.0.                                                                                                                                                                                                   |
| "Metrics expose user or session information."                               | Treat the metrics listener as a separate operational endpoint. Verify bind address, network reachability, labels, and whether the deployment provides authentication or network isolation.                                                                                                                                                    |
| "A vulnerability exists in Spark, Hadoop, Kubernetes, LDAP, or a database." | Determine whether Kyuubi introduces an unsafe integration, passes attacker-controlled input into the vulnerable path, or distributes the affected component. Otherwise classify it as `DEPENDENCY-OR-PLATFORM`.                                                                                                                               |

## Existing Controls and Evidence

Verify these controls when they apply to a report:

- Authentication methods and their defaults are registered in
  `kyuubi-common/.../config/KyuubiConf.scala`. The current default is
  `kyuubi.authentication=NONE`. Do not describe a deployment with this default
  as a secure shared-tenant deployment.
- REST `/v1/*` requests pass through `AuthenticationFilter`. Administrator
  resources check `isAdministrator`. The REST web UI and engine-UI proxy use
  separate handlers. The Trino frontend uses a separate HTTP service. See
  `kyuubi-server/.../http/authentication/AuthenticationFilter.scala`,
  `kyuubi-server/.../api/v1/AdminResource.scala`, and
  `kyuubi-server/.../KyuubiTrinoFrontendService.scala`.
- The REST service separates the authenticated real user from the session user
  or proxy user. It also verifies proxy access. Authentication does not prove
  object ownership. The authentication layer does not do the same owner check
  for each handle-based route. Verify the applicable session, operation, batch,
  result, or log route. See
  `kyuubi-server/.../KyuubiRestFrontendService.scala` and
  [`docs/client/rest/rest_api.md`](../client/rest/rest_api.md).
- Thrift session creation applies the same real-user/session-user distinction
  and proxy check in `kyuubi-common/.../service/TFrontendService.scala`.
- Thrift HTTP also supports a signed authentication cookie and a `doAs` query
  parameter that enters proxy-user validation. Cookie authentication defaults
  to enabled. Its XSRF filter defaults to disabled. Check all three settings for
  G1 or G2.
- The Trino frontend reads a request-user header into its request context and
  does not share the REST authentication filter. Treat that header as an
  untrusted identity claim until the deployment supplies an independent
  authentication boundary.
- Native frontend TLS is configurable for Thrift binary and Thrift HTTP but is
  not enabled by default. REST and Trino use plain Jetty HTTP and depend on an
  external TLS terminator or network isolation. SASL authentication, integrity,
  and confidentiality are separate quality-of-protection choices. See
  `kyuubi-common/.../config/KyuubiConf.scala` and the transport guidance in
  [`docs/security/ldap.md`](ldap.md) and
  [`docs/client/advanced/kerberos.md`](../client/advanced/kerberos.md).
- Internal server-engine and server-server communication can use a secret and a
  short-lived token. This feature is disabled by default. The token identifier
  contains issue and expiry times. Verify cryptographic integrity, peer or
  audience binding, replay behavior, and downgrade behavior. Encryption alone
  does not provide authentication. See
  [`docs/security/internal_secure_access.md`](internal_secure_access.md).
- ZooKeeper authentication and engine authentication default to `NONE`, and
  etcd TLS defaults to false. HA-store authentication, transport, namespace,
  and ACLs are separate controls from Kyuubi's internal engine token. See
  `kyuubi-ha/.../HighAvailabilityConf.scala`.
- Kerberos keytabs, ticket caches, and Hadoop delegation tokens are security
  sensitive. The Kinit guide warns that credential paths must be excluded from
  user-accessible local directories. See
  [`docs/security/kinit.md`](kinit.md) and
  [`docs/security/hadoop_credentials_manager.md`](hadoop_credentials_manager.md).
- The local-directory allow list is empty by default. The redaction regular
  expression is optional. REST configuration responses support `ORIGINAL`,
  `REDACTED`, and `NONE` modes. Check these settings when you assess secret
  exposure. A documentation warning does not enforce a control.
- Batch resource uploads are enabled by default and their maximum file sizes
  are unlimited by default. Check connection, engine-startup, upload,
  temporary-file, and query limits during availability triage.
- The Helm chart uses a service account and a namespaced role for engine pod
  operations. The deployment guides also contain broader `edit` and
  `clusterrolebinding` examples. Treat these examples as official security
  guidance when you assess privilege exposure. See
  [the Helm values file](https://github.com/apache/kyuubi/blob/master/charts/kyuubi/values.yaml),
  [`docs/deployment/kyuubi_on_kubernetes.md`](../deployment/kyuubi_on_kubernetes.md),
  and [`docs/deployment/engine_on_kubernetes.md`](../deployment/engine_on_kubernetes.md).
- Prometheus metrics use a separate HTTP listener. This listener does not use
  the frontend authentication filter. Kyuubi also supports JMX, JSON, console,
  and SLF4J reporters. Assess the destination, permissions, labels, and exposure
  of each enabled reporter.
- The engine UI proxy is disabled by default, so the web UI normally opens an
  engine's native URL directly. Direct and proxied engine UIs have independent
  authentication, network, and action boundaries that must be assessed.
- The Spark authorization extension delegates policy decisions to Ranger and
  supports policy auditing. See
  [`docs/security/authorization/spark/install.md`](authorization/spark/install.md).
- The Data Agent defaults to the local `ECHO` provider. When an
  OpenAI-compatible or custom external provider and a JDBC data source are
  configured, it can send conversation context to that provider. It can also
  run SQL against the data source. It supports `AUTO_APPROVE`, `NORMAL`, and
  `STRICT` tool approval modes. Kyuubi keeps chat history and data-source data
  in the session and browser storage. See
  [`docs/quick_start/quick_start_with_data_agent.md`](../quick_start/quick_start_with_data_agent.md).

These references define expected behavior. They do not prove that each version
or deployment has this behavior. Inspect the affected version and
configuration.

## Residual Risks

- The default authentication method is `NONE`. Transport encryption is
  optional. A secure shared deployment depends on operator configuration and
  deployment isolation.
- The Trino frontend, REST web UI, engine-UI proxy, and Prometheus listener do
  not all share the REST `/v1/*` authentication path. Each endpoint needs an
  explicit deployment boundary and authorization review.
- Kyuubi delegates substantial authorization to Hadoop, Spark, Ranger,
  Kubernetes, YARN, storage, metadata, and identity services. A complete
  assessment requires the deployment profile, not just Kyuubi source code.
- Internal engine tokens and HA-store authentication are separate controls.
  Internal security does not enable ZooKeeper or etcd authentication. It also
  does not enable ACLs, peer binding, or replay protection.
- Engine sharing and proxy-user behavior are security-sensitive and vary by
  share level and cluster policy. Check the configured profile before you
  assess a report.
- The Data Agent adds a model-provider, prompt, data-source, browser-history,
  and tool-approval boundary.
- An application-level availability boundary is difficult to establish for
  arbitrary SQL workloads. A report must show a service crash, a limit bypass,
  or an effect on another tenant. High resource use alone does not qualify.

## Open Policy Decisions

The following items require explicit Kyuubi project policy:

1. Which Kyuubi versions and release branches are covered by security fixes?
2. Is `NONE`/`NOSASL` considered supported only for isolated or trusted-user
   deployments, and where is that limitation stated normatively?
3. What application-level availability threshold qualifies for security
   handling, including cross-tenant resource starvation and engine-creation
   floods?
4. Which engine, connector, UDF, JAR, and batch-resource capabilities are
   trusted deployment inputs, and which must be treated as tenant-controlled?
5. What are the authentication and ownership rules for the Trino frontend,
   REST web UI, engine-UI proxy, metrics listener, and every REST resource?
6. What cryptographic and ACL properties are required for internal engine
   tokens and ZooKeeper or etcd service discovery?

## Review Cadence

Review this document when any of the following changes:

- A frontend, authentication method, proxy-user rule, REST authorization rule,
  engine-sharing mode, or internal security protocol changes.
- Kyuubi adds a cluster-manager integration, engine type, credential provider,
  data-source integration, or official deployment artifact.
- Helm RBAC, container privileges, CI release workflow, dependency policy, or
  secret handling changes.
- A security report reveals a missing threat row, incorrect trust assumption,
  or a false-positive triage rule.

A maintainer must review each material change. The review must cover the
affected security goals, trust boundaries, threat rows, and triage examples.
