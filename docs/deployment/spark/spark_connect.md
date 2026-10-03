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

# Spark Connect in the Spark SQL Engine

The Spark SQL engine can start a Spark Connect service next to its Thrift frontend, so that
Spark Connect clients reach a Kyuubi-managed engine: the same `SparkContext`, the same session
extensions and so the same authorization rules a JDBC client gets. Spark gives each Connect session
its own `SparkSession`, so temporary views, registered functions and SQL configs are not shared with
the JDBC sessions of that engine.

The feature is off by default and is enabled per engine:

```properties
kyuubi.engine.spark.connect.enabled=true
spark.connect.authenticate.token=<a token this deployment generates>
```

`kyuubi.engine.spark.connect.enabled` is immutable and the server decides it per engine, so a
session cannot turn Spark Connect on by itself — neither under that key nor under the
`spark.kyuubi.` prefixed copy that the engine would otherwise read.

## Requirements

|                                            Requirement                                             |                                                                                                                         Why                                                                                                                          |
|----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Spark 4.0 or later, with the `spark-connect` jar in the distribution                               | `spark.connect.authenticate.token` was added in Spark 4.0. Spark 3.5 serves Spark Connect without authentication, so the engine refuses to start rather than open an endpoint that authenticates no one                                              |
| `spark.connect.authenticate.token`, or the `SPARK_CONNECT_AUTHENTICATE_TOKEN` environment variable | Every client that reaches the engine is authenticated by this pre-shared token                                                                                                                                                                       |
| `kyuubi.engine.share.level=USER` and `kyuubi.engine.doAs.enabled=true`                             | A plan submitted over Spark Connect runs as the user the engine runs as, not as the `user_id` the client sends. Only these two together make that the session user; an engine started any other way serves Thrift only and says so in its launch log |

A runtime that cannot authenticate Spark Connect clients, or a missing token, fails the engine with
a message naming what is missing: the deployment asked for Connect and cannot have it. The share
level is the session's own choice, so an engine that would not run as the session user starts
without Connect instead of failing.

## What the engine configures

Unless the deployment sets them, the engine adds:

- `spark.plugins` gains `org.apache.spark.sql.connect.SparkConnectPlugin`, which is how Spark
  starts Connect inside the driver;
- `spark.connect.grpc.binding.port=0`, so that engines of different users on the same host do not
  compete for a fixed port. The port that was bound is logged as `sc://<driver host>:<port>`.

Configs the deployment sets are kept, so a fixed port or extra plugins keep working.

## Limitations

- The Spark Connect endpoint is not advertised in engine discovery yet, so clients need the
  address from the engine log.
- Engine idle timeout and graceful shutdown still count Thrift sessions only. An engine whose only
  client is a Spark Connect session can be terminated as idle.
- The token is passed to the engine like any other Spark config, so it is visible in the driver's
  command line.
- On a Spark distribution without [SPARK-58658](https://issues.apache.org/jira/browse/SPARK-58658),
  an authenticated Spark Connect client reads the engine's configuration back through the Config
  RPC, and the secrets the server hands the engine are in it —
  `spark.kyuubi.ha.zookeeper.auth.digest` among them. The engine names them in
  `spark.redaction.regex`, but only the patched handler consults that pattern. The fix is merged
  on every Spark branch and released in none: 4.0.4, 4.1.3 and 4.2.0 all predate it, so it arrives
  in the next patch release of each line.
- Share levels other than `USER`, and engines started with `kyuubi.engine.doAs.enabled=false`, do
  not serve Spark Connect.

