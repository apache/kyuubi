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

# Dangerous Join Watchdog

Kyuubi Dangerous Join Watchdog detects risky join planning patterns before query execution.
It helps reduce accidental Cartesian products, oversized broadcast attempts, and long-running nested loop joins.

## Background

In shared SQL gateway environments, a single risky join can consume excessive driver memory or create very slow jobs.
The Dangerous Join Watchdog adds planning-time checks for these high-risk patterns.

## Risk Rules

### Equi-Join

- Rule 1: Equi-join is marked dangerous when it degrades to a Cartesian pattern.
- Rule 2: Equi-join is marked dangerous when the estimated build side exceeds the configured broadcast ratio threshold.

### Non-Equi Join

- Rule 1: Non-equi join is marked dangerous when both sides exceed broadcast threshold and effectively become Cartesian risk.
- Rule 2: Non-equi join is marked dangerous when build side is not selectable and the plan falls back to a second BNLJ pattern.

## Configurations

|                      Name                      | Default |                                  Meaning                                  |
|------------------------------------------------|---------|---------------------------------------------------------------------------|
| `kyuubi.watchdog.dangerousJoin.enabled`        | `true`  | Enable or disable dangerous join detection                                |
| `kyuubi.watchdog.dangerousJoin.broadcastRatio` | `0.8`   | Ratio against Spark broadcast threshold for warning/reject decision       |
| `kyuubi.watchdog.dangerousJoin.action`         | `WARN`  | `WARN` logs diagnostics; `REJECT` throws exception and rejects submission |

## Usage

1. Put Kyuubi Spark extension jar into Spark classpath.
2. Configure SQL extensions:

```properties
spark.sql.extensions=org.apache.kyuubi.sql.KyuubiSparkSQLExtension,org.apache.kyuubi.sql.watchdog.KyuubiDangerousJoinExtension
```

3. Configure action:

```properties
kyuubi.watchdog.dangerousJoin.action=WARN
```

or

```properties
kyuubi.watchdog.dangerousJoin.action=REJECT
```

## Sample WARN Log

When action is `WARN`, Kyuubi writes a structured JSON payload:

```text
KYUUBI_LOG_KEY={"sql":"SELECT ...","joinType":"INNER","reason":"Cartesian","leftSize":10485760,"rightSize":15728640,"broadcastThreshold":10485760,"broadcastRatio":0.8}
```

## Sample REJECT Error

When action is `REJECT`, query submission fails with:

```text
errorCode=41101
Query rejected due to dangerous join strategy: {...details...}
```

## Disable or Tune

- Disable watchdog:

```properties
kyuubi.watchdog.dangerousJoin.enabled=false
```

- Increase tolerance:

```properties
kyuubi.watchdog.dangerousJoin.broadcastRatio=0.95
```

## FAQ

### What if `spark.sql.adaptive.enabled=true`?

Dangerous Join Watchdog runs in planner strategy phase and evaluates pre-execution plan statistics.
AQE may still optimize runtime plans, but watchdog decisions are made before query execution starts.
