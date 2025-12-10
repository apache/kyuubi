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

# Shutdown Watchdog Plugin

The shutdown watchdog prevents zombie Spark SQL engines by monitoring graceful shutdown.
If the driver does not finish within the configured timeout, the plugin produces a JVM thread
dump for diagnostics and forcefully terminates the process to unblock resource reclamation.

## Build with Apache Maven

The plugin lives in the module `extensions/spark/kyuubi-spark-shutdown-watchdog` and can be built via:

```shell
build/mvn clean package -DskipTests \
  -pl extensions/spark/kyuubi-spark-shutdown-watchdog -am
```

Because the implementation is pure Java, there are no Scala runtime
dependencies. You can build it with Spark Scala 2.12 libraries and use the
resulting jar with Spark applications running on Scala 2.13 (or vice versa).

After the build succeeds the jar is located at:
`./extensions/spark/kyuubi-spark-shutdown-watchdog/target/kyuubi-spark-shutdown-watchdog-${project.version}.jar`

## Installing

Place the jar on the Spark driver classpath, for example by:

- Copying it to `$SPARK_HOME/jars`, or
- Pointing Spark to it through `spark.jars`

## Enabling the plugin

Add the plugin class to `spark.plugins` when launching the Spark SQL engine:

```properties
spark.plugins=org.apache.spark.kyuubi.shutdown.watchdog.SparkShutdownWatchdogPlugin
```

Configure the timeout directly through Spark:

```properties
spark.kyuubi.shutdown.watchdog.timeout=1m
```

Tune this value according to how long you expect the engine to take during a
normal shutdown; set `0` or a negative value to disable forced termination. In
practice pick a value greater than other engine shutdown knobs (executor drain,
engine pool shutdown, etc.) so that the watchdog only fires when everything else
has already stalled.

## Configuration

|           Configuration Key            | Default |                                                Description                                                 |
|----------------------------------------|---------|------------------------------------------------------------------------------------------------------------|
| spark.kyuubi.shutdown.watchdog.enabled | true    | Enables/disables the plugin globally.                                                                      |
| spark.kyuubi.shutdown.watchdog.timeout | 0       | Maximum wait (milliseconds) for graceful shutdown before forcing termination. `0` or negative disables it. |

## Behavior on timeout

When the timeout elapses the plugin:

1. Emits a detailed JVM thread dump to the Spark driver logs.
2. Terminates the driver with Spark's standard `SparkExitCode.UNCAUGHT_EXCEPTION` (the same code the
   driver would use for an uncaught fatal error). If the watchdog itself ever fails it will exit
   with `SparkExitCode.UNCAUGHT_EXCEPTION_TWICE`.

This keeps the shutdown semantics consistent with the rest of Spark, making it easier for cluster
managers (YARN/Kubernetes) to treat the forced exit as a regular Spark failure.

