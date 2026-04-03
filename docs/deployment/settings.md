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

# Deployment Settings for Dangerous Join Watchdog

## Spark SQL Extensions

```properties
spark.sql.extensions=org.apache.kyuubi.sql.KyuubiSparkSQLExtension,org.apache.kyuubi.sql.watchdog.KyuubiDangerousJoinExtension
```

## Dangerous Join Configurations

|                      Name                      | Default |                        Description                        |
|------------------------------------------------|---------|-----------------------------------------------------------|
| `kyuubi.watchdog.dangerousJoin.enabled`        | `true`  | Enable dangerous join watchdog                            |
| `kyuubi.watchdog.dangerousJoin.broadcastRatio` | `0.8`   | Broadcast threshold coefficient                           |
| `kyuubi.watchdog.dangerousJoin.action`         | `WARN`  | `WARN` only logs diagnostics, `REJECT` throws error 41101 |

For detailed rules and examples, see [Dangerous Join Watchdog](../watchdog/dangerous-join.md).
