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

# Enabling Engine Pool

## Overview

Engine Pool is a Kyuubi feature designed to improve concurrency, throughput,
and resource utilization. It achieves this by load-balancing user sessions
across a pool of multiple engine instances, instead of concentrating them on
a single one. This distribution is managed through a dynamic subdomain allocation
mechanism.

Engine pool works by creating multiple engine subdomains (e.g.,
`engine-pool-0`, `engine-pool-1`) and distributing sessions across these
subdomains. Engines are created on-demand when first accessed, not
pre-created.

## Important Notes

When using engine pool, the subdomain behavior depends on whether you have
configured `kyuubi.engine.share.level.subdomain`:

- **With subdomain configured**: If you set
  `kyuubi.engine.share.level.subdomain=your-subdomain`, this configuration
  will be used even when engine pool is enabled
- **Without subdomain configured**: The system will automatically generate
  subdomains using the engine pool naming pattern (e.g., `engine-pool-0`,
  `engine-pool-1`, `my-pool-0`, `my-pool-1`)

## Configuration Parameters

|              Parameter              | Default Value | Required |                                                                                                         Description                                                                                                          |
|-------------------------------------|---------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `kyuubi.engine.pool.size`           | -1 (disabled) | Yes      | The size of the engine pool. Must be greater than 0 to enable engine pool                                                                                                                                                    |
| `kyuubi.engine.share.level`         | USER          | No       | Engine sharing level. Supports USER, GROUP, SERVER, SERVER_LOCAL. **NOT** supported with CONNECTION                                                                                                                          |
| `kyuubi.engine.pool.name`           | engine-pool   | No       | The name of the engine pool                                                                                                                                                                                                  |
| `kyuubi.engine.pool.selectPolicy`   | RANDOM        | No       | Engine selection policy: RANDOM or POLLING                                                                                                                                                                                   |
| `kyuubi.engine.pool.size.threshold` | 9             | No       | Server-side upper limit of engine pool size. This acts as a safety guardrail for administrators to prevent users from requesting excessively large engine pools that could exhaust cluster resources (server-only parameter) |

## Configuration Precedence

Configurations follow a clear hierarchy. Session-level settings provided
during connection will override the settings in the `kyuubi-defaults.conf`
file. Note that `kyuubi.engine.pool.size.threshold` is a server-only
parameter and cannot be overridden by sessions.

## Configuration Steps

### Configure Engine Pool Settings

Add the following configurations to your `kyuubi-defaults.conf` file:

```properties
# Required - Enable engine pool by setting pool size greater than 0
kyuubi.engine.pool.size=3
# Optional - Set engine sharing level
kyuubi.engine.share.level=USER
# Optional - Set engine pool name
kyuubi.engine.pool.name=my-engine-pool
# Optional - Set engine selection policy (RANDOM or POLLING)
kyuubi.engine.pool.selectPolicy=RANDOM
# Optional - Server-side engine pool size threshold (server-only parameter)
kyuubi.engine.pool.size.threshold=9
```

### Restart Server

After modifying the configuration, restart the Server to apply the changes:

```bash
# Stop Server
$KYUUBI_HOME/bin/kyuubi stop

# Start Server
$KYUUBI_HOME/bin/kyuubi start
```

## Configuration Examples

### Basic Engine Pool Configuration

```properties
# Enable engine pool with 2 engines
kyuubi.engine.pool.size=2
```

### Advanced Engine Pool Configuration

```properties
# Enable engine pool with custom settings
kyuubi.engine.pool.size=5
kyuubi.engine.share.level=USER
kyuubi.engine.pool.name=spark-engine-pool
kyuubi.engine.pool.selectPolicy=POLLING
kyuubi.engine.pool.size.threshold=10
```

