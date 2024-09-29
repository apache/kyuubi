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

# JVM Quake Support

When facing out-of-control memory management in Spark engine, we typically use `spark.driver/executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath={heapDumpPath} -XX:OnOutOfMemoryError="kill -9 %p"` as a remedy by killing the process and generating a heap dump for post-analysis. However, even with jvm kill protection, we may still encounter issues caused by JVM running out of memory, such as repeated execution of Full GC without performing any useful work during the pause time. Since the JVM does not exhaust 100% of resources, JVMkill will not be triggered.

So introducing JVMQuake provides more granular monitoring of GC behavior, enabling early detection of memory management issues and facilitating fast failure.

## Usage

JVM Quake is implemented through Spark plugins, This plugin technically supports Spark 3.0 onwards, but was only verified with Spark 3.3 to 4.0 in CI.

### Build with Apache Maven

Spark JVM Quake Plugins is built using [Apache Maven](https://maven.apache.org).
To build it, `cd` to the root directory of kyuubi project and run:

```shell
build/mvn clean package -DskipTests -pl :kyuubi-spark-jvm-quake_2.12 -am
```

After a while, if everything goes well, you will get the plugin under `./extensions/spark/kyuubi-spark-jvm-quake/target/kyuubi-spark-jvm-quake_${scala.binary.version}-${project.version}.jar`

### Installing

With the `kyuubi-spark-jvm-quake_*.jar` and its transitive dependencies available for spark runtime classpath, such as
- Copied to `$SPARK_HOME/jars`, or
- Specified to `spark.jars` configuration

### Settings for Spark Plugins

Add `org.apache.spark.kyuubi.jvm.quake.SparkJVMQuakePlugin` to the spark configuration `spark.plugins`.

```properties
spark.plugins=org.apache.spark.kyuubi.jvm.quake.SparkJVMQuakePlugin
```

## Additional Configurations

|                   Name                   |       Default Value       |                            Description                            |
|------------------------------------------|---------------------------|-------------------------------------------------------------------|
| spark.driver.jvmQuake.enabled            | false                     | when true, enable driver jvmQuake                                 |
| spark.executor.jvmQuake.enabled          | false                     | when true, enable executor jvmQuake                               |
| spark.driver.jvmQuake.heapDump.enabled   | false                     | when true, enable jvm heap dump when jvmQuake reach the threshold |
| spark.executor.jvmQuake.heapDump.enabled | false                     | when true, enable jvm heap dump when jvmQuake reach the threshold |
| spark.jvmQuake.killThreshold             | 200                       | The number of seconds to kill process                             |
| spark.jvmQuake.exitCode                  | 502                       | The exit code of kill process                                     |
| spark.jvmQuake.heapDumpPath              | /tmp/spark_jvm_quake/apps | The path of heap dump                                             |
| spark.jvmQuake.checkInterval             | 3                         | The number of seconds to check jvmQuake                           |
| spark.jvmQuake.runTimeWeight             | 1.0                       | The weight of rum time                                            |

