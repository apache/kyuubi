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


# Building Kyuubi

## Building Kyuubi with Apache Maven

**Kyuubi** is built based on [Apache Maven](http://maven.apache.org),

```bash
./build/mvn clean package -DskipTests
```

This results in the creation of all sub-modules of Kyuubi project without running any unit test.

If you want to test it manually, you can start Kyuubi directly from the Kyuubi project root by running

```bash
bin/kyuubi start
```

## Building a Submodule Individually

For instance, you can build the Kyuubi Common module using:

```bash
build/mvn clean package -pl kyuubi-common -DskipTests
```

## Building Submodules Individually

For instance, you can build the Kyuubi Common module using:

```bash
build/mvn clean package -pl kyuubi-common,kyuubi-ha -DskipTests
```

## Skipping Some modules

For instance, you can build the Kyuubi modules without Kyuubi Codecov and Assembly modules using:

```bash
mvn clean install -pl '!dev/kyuubi-codecov,!kyuubi-assembly' -DskipTests
```

## Building Kyuubi against Different Apache Spark versions

Since v1.1.0, Kyuubi support building with different Spark profiles,

Profile | Default  | Since
--- | --- | --- 
-Pspark-3.1 | No | 1.1.0
-Pspark-3.2 | No | 1.4.0
-Pspark-3.3 | Yes | 1.6.0


## Building with Apache dlcdn site

By default, we use `https://archive.apache.org/dist/` to download the built-in release packages of engines,
such as Spark or Flink.
But sometimes, you may find it hard to reach, or the download speed is too slow,
then you can define the `apache.archive.dist` by `-Pmirror-cdn` to accelerate to download speed.
For example,

```bash
build/mvn clean package -Pmirror-cdn
```

The profile migrates your download repo to the Apache offically suggested site - https://dlcdn.apache.org.
Note that, this site only holds the latest versions of Apache releases. You may fail if the specific version
defined by `spark.version` or `flink.version` is overdue.
