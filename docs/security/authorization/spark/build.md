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

# Building Kyuubi Spark AuthZ Plugin

## Build with Apache Maven

Kyuubi Spark AuthZ Plugin is built using [Apache Maven](https://maven.apache.org).
To build it, `cd` to the root direct of kyuubi project and run:

```shell
build/mvn clean package -pl :kyuubi-spark-authz_2.12 -am -DskipTests
```

After a while, if everything goes well, you will get the plugin finally in two parts:

- The main plugin jar, which is under `./extensions/spark/kyuubi-spark-authz/target/kyuubi-spark-authz_${scala.binary.version}-${project.version}.jar`
- The least transitive dependencies needed, which are under `./extensions/spark/kyuubi-spark-authz/target/scala-${scala.binary.version}/jars`

## Build shaded jar with Apache Maven

Apache Kyuubi also provides the shaded jar for the Spark AuthZ plugin, You can run the AuthZ plugin using just a shaded jar without the additional dependency of jars,
To build it, `cd` to the root direct of kyuubi project and run:

```shell
build/mvn clean package -pl :kyuubi-spark-authz-shaded_2.12 -am -DskipTests
```

After a while, if everything goes well, you will get the plugin finally:

- The shaded AuthZ plugin jar, which is under `./extensions/spark/kyuubi-spark-authz-shaded/target/kyuubi-spark-authz-shaded_${scala.binary.version}-${project.version}.jar`

### Build against Different Apache Spark Versions

The maven option `spark.version` is used for specifying Spark version to compile with and generate corresponding transitive dependencies.
By default, it is always built with the latest `spark.version` defined in kyuubi project main pom file.
Sometimes, it may be incompatible with other Spark distributions, then you may need to build the plugin on your own targeting the Spark version you use.

For example,

```shell
build/mvn clean package -pl :kyuubi-spark-authz_2.12 -am -DskipTests -Pspark-3.4 -Dspark.version=3.4.1
```

The available `spark.version`s are shown in the following table.

|   Spark Version   | Supported |                                                         Remark                                                         |
|:-----------------:|:---------:|:----------------------------------------------------------------------------------------------------------------------:|
|      master       |     √     |                                                           -                                                            |
|       3.5.x       |     √     |                                                           -                                                            |
|       3.4.x       |     √     |                                                           -                                                            |
|       3.3.x       |     √     |                                                           -                                                            |
|       3.2.x       |     √     |                                                           -                                                            |
|       3.1.x       |     x     |                                                   EOL since v1.10.0                                                    |
|       3.0.x       |     x     |                                                    EOL since v1.9.0                                                    |
| 2.4.x and earlier |     ×     | [PR 2367](https://github.com/apache/kyuubi/pull/2367) is used to track how we work with older releases with scala 2.11 |

Currently, Spark released with Scala 2.12 are supported.

### Build against Different Apache Ranger Versions

The maven option `ranger.version` is used for specifying Ranger version to compile with and generate corresponding transitive dependencies.
By default, it is always built with the latest `ranger.version` defined in kyuubi project main pom file.
Sometimes, it may be incompatible with other Ranger Admins, then you may need to build the plugin on your own targeting the Ranger Admin version you connect with.

```shell
build/mvn clean package -pl :kyuubi-spark-authz_2.12 -am -DskipTests -Dranger.version=2.4.0
```

The available `ranger.version`s are shown in the following table.

| Ranger Version | Supported |                                          Remark                                           |
|:--------------:|:---------:|:-----------------------------------------------------------------------------------------:|
|     2.5.x      |     √     |                                             -                                             |
|     2.4.x      |     √     |                                             -                                             |
|     2.3.x      |     √     |                                             -                                             |
|     2.2.x      |     √     |                                             -                                             |
|     2.1.x      |     √     |                                             -                                             |
|     2.0.x      |     √     |                                             -                                             |
|     1.2.x      |     √     |                                             -                                             |
|     1.1.x      |     √     |                                             -                                             |
|     1.0.x      |     √     |                                             -                                             |
|     0.7.x      |     √     |                                             -                                             |
|     0.6.x      |     X     | [KYUUBI-4672](https://github.com/apache/kyuubi/issues/4672) reported unresolved failures. |

Currently, all ranger releases are supported.

## Test with ScalaTest Maven plugin

If you omit `-DskipTests` option in the command above, you will also get all unit tests run.

```shell
build/mvn clean package -pl :kyuubi-spark-authz_2.12
```

If any bug occurs and you want to debug the plugin yourself, you can configure `-DdebugForkedProcess=true` and `-DdebuggerPort=5005`(optional).

```shell
build/mvn clean package -pl :kyuubi-spark-authz_2.12 -DdebugForkedProcess=true
```

The tests will suspend at startup and wait for a remote debugger to attach to the configured port.

We will appreciate if you can share the bug or the fix to the Kyuubi community.
