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

# Internal Secure Access

Kyuubi supports secure communication channels for all internal interactions, both:

* Between the Kyuubi server and Kyuubi engines.
* Between Kyuubi server instances (REST API high availability mode).

If `kyuubi.internal.security.enabled` is set to `true`, all internal communication are encrypted.
Currently, it's enforced to set `kyuubi.internal.security.enabled` to `true` if you want to use
REST API high availability mode.

## Managing encryption secrets

Encryption secrets are managed by a subclass of `org.apache.kyuubi.service.authentication.EngineSecuritySecretProvider`,
which can be configured via `kyuubi.internal.security.secret.provider`.

Kyuubi provides the following built-in implementations:

* simple: Use the pre-shared secret configured by `kyuubi.internal.security.secret.provider.simple.secret`.
* zookeeper: Use the secret stored in the ZooKeeper znode configured by `kyuubi.ha.zookeeper.engine.secure.secret.node`.

We strongly recommend restricting access to these secrets.

* If using the `simple` provider, you should also configure `kyuubi.server.redaction.regex`
  (e.g., `(?i)secret|password|token|access[.]?key`) to redact the secret.
* If using the `zookeeper` provider, ensure that ACLs (Access Control Lists) are properly configured for the znode
  where the secret is stored.

Custom implementations of `EngineSecuritySecretProvider` are also supported for advanced secret management needs.

## Rolling upgrades from Kyuubi 1.11.1 or earlier

Kyuubi 1.12 changed the internal access token format from a ciphertext encrypted with a static zero IV to a
ciphertext prefixed with a random IV. To interoperate with servers and engines from Kyuubi 1.11.1 or earlier,
use the following rolling upgrade procedure:

1. Configure every replacement Kyuubi server with
   `kyuubi.internal.security.crypto.compatibilityMode=MIGRATE`. The configuration is propagated to newly
   launched engines. In this mode, Kyuubi issues legacy zero-IV tokens and accepts both token formats.
2. Replace all pre-1.12 servers and engines, then ensure that no pre-1.12 server or engine remains.
3. Change the compatibility mode to `READ_COMPATIBLE` on every server, restart the servers, and recycle
   any remaining engines. This mode restores random-IV token issuance while continuing to accept legacy
   tokens.
4. After at least the maximum `kyuubi.internal.security.token.max.lifetime` configured on any pre-1.12
   peer, plus any clock skew between peers, change the mode to `STRICT` and restart every server.

The compatibility mode is read only at process startup. `MIGRATE` weakens token security and should be limited
to the migration window. The pre-1.12 format used the platform-default charset, so the rolling upgrade requires
ASCII encryption secrets. `MIGRATE` cannot interoperate with an unpatched Kyuubi 1.12.0 or later component that
is still using `STRICT`, because that component accepts only random-IV tokens. Plan the migration from a
homogeneous pre-1.12 deployment or isolate components that require different compatibility modes.

## Configurations

|                           Key                            |       Default        |                                                               Meaning                                                               |   Type   | Since  |
|----------------------------------------------------------|----------------------|-------------------------------------------------------------------------------------------------------------------------------------|----------|--------|
| `kyuubi.internal.security.enabled`                       | false                | Whether to enable secure access across all the internal communications.                                                             | boolean  | 1.12.0 |
| `kyuubi.internal.security.token.max.lifetime`            | PT10M                | The max lifetime of the token used for internal secure access.                                                                      | duration | 1.12.0 |
| `kyuubi.internal.security.secret.provider`               | zookeeper            | The class used to manage the internal security secret.                                                                              | string   | 1.12.0 |
| `kyuubi.internal.security.secret.provider.simple.secret` | &lt;undefined&gt;    | The secret key used for internal security access when `kyuubi.internal.security.secret.provider` is set to `simple`.                | string   | 1.12.0 |
| `kyuubi.internal.security.crypto.keyAlgorithm`           | AES                  | The algorithm for generated secret key.                                                                                             | string   | 1.12.0 |
| `kyuubi.internal.security.crypto.keyLength`              | 128                  | The length in bits of the encryption key to generate. Valid values are 128, 192, and 256.                                           | int      | 1.12.0 |
| `kyuubi.internal.security.crypto.cipher`                 | AES/CBC/PKCS5PADDING | The cipher transformation to use for encrypting internal access token.                                                              | string   | 1.12.0 |
| `kyuubi.internal.security.crypto.compatibilityMode`      | STRICT               | Controls internal access token compatibility during rolling upgrades. Supported values: `STRICT`, `READ_COMPATIBLE`, and `MIGRATE`. | string   | 1.12.1 |
| `kyuubi.internal.security.crypto.ivLength`               | 16                   | Initial vector length, in bytes.                                                                                                    | int      | 1.12.0 |

