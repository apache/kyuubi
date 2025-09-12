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

# Building From Source

## Building With Maven

**Kyuubi** is built based on [Apache Maven](https://maven.apache.org),

```bash
./build/mvn clean package -DskipTests
```

This results in the creation of all sub-modules of Kyuubi project without running any unit test.

If you want to test it manually, you can start Kyuubi directly from the Kyuubi project root by running

```bash
bin/kyuubi start
```

## Building A Submodule Individually

For instance, you can build the Kyuubi Common module using:

```bash
build/mvn clean package -pl kyuubi-common -DskipTests
```

## Building Submodules Individually

For instance, you can build the Kyuubi Common module using:

```bash
build/mvn clean package -pl kyuubi-common,kyuubi-ha -DskipTests
```

## Skipping Some Modules

For instance, you can build the Kyuubi modules without Kyuubi Codecov and Assembly modules using:

```bash
mvn clean install -pl '!dev/kyuubi-codecov,!kyuubi-assembly' -DskipTests
```

## Building Kyuubi Against Different Apache Spark Versions

Since v1.1.0, Kyuubi support building with different Spark profiles,

|   Profile   | Default | Since |
|-------------|---------|-------|
| -Pspark-3.3 |         | 1.6.0 |
| -Pspark-3.4 |         | 1.8.0 |
| -Pspark-3.5 | ✓       | 1.8.0 |

## Building Kyuubi Against Different Scala Versions

Since v1.8.0, Kyuubi support building with different Scala profile. Currently, Kyuubi supports building with Scala 2.12 and 2.13, while Scala 2.12 by default.

|   Profile    | Default | Since |
|--------------|---------|-------|
| (Scala 2.12) | ✓       | -     |
| -Pscala-2.13 |         | 1.8.0 |

Please activate `scala-2.13` profile when Scala 2.13 support is needed. The GA tests have covered integration test with the Kyuubi server, engines and related plugins, while the Flink engine and it's integration tests are not included for the reason that Flink does not support Scala 2.13 yet and will pull out client support for Scala.

For the Scala version for Spark engines, the server will look up the `SPARK_SCALA_VERSION` system environment variable first, and then the Scala version of the server compiled with if the former one not set. For the Scala version for other engines, the server will use the Scala version of the server compiled with.

## Building With Apache dlcdn Site

By default, we use [`closer.lua`](https://infra.apache.org/release-download-pages.html#download-scripts) to download the built-in release packages of engines,
such as Spark or Flink.
But sometimes, you may find it hard to reach, or the download speed is too slow,
then you can define the `apache.archive.dist` by `-Pmirror-cdn` to accelerate to download speed.
For example,

```bash
build/mvn clean package -Pmirror-cdn
```

The profile migrates your download repo to the Apache officially suggested site - https://dlcdn.apache.org.
Note that, this site only holds the latest versions of Apache releases. You may fail if the specific version
defined by `spark.version` or `flink.version` is overdue.

## Building with the `fast` profile

The `fast` profile helps to significantly reduce build time, which is useful for development or compilation validation, by skipping running the tests, code style checks, building scaladoc, enforcer rules and downloading engine archives used for tests.

```bash
build/mvn clean package -Pfast
```

