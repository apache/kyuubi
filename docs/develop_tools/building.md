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

<div align=center>

![](../imgs/kyuubi_logo.png)

</div>

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
build/mvn clean package -pl :kyuubi-common -DskipTests
```

## Building Submodules Individually

For instance, you can build the Kyuubi Common module using:

```bash
build/mvn clean package -pl :kyuubi-common,:kyuubi-ha -DskipTests
```

## Skipping Some modules

For instance, you can build the Kyuubi modules without Kyuubi Codecov and Assembly modules using:

```bash
 mvn clean install -pl '!:kyuubi-codecov,!:kyuubi-assembly' -DskipTests
```

## Building Kyuubi against Different Apache Spark versions

Since v1.1.0, Kyuubi support building with different Spark profiles,

Profile | Default  | Since
--- | --- | --- 
-Pspark-3.0 | Yes | 1.0.0
-Pspark-3.1 | No | 1.1.0


## Defining the Apache Mirror for Spark or Flink

By default, we use `https://archive.apache.org/dist/spark/` to download the built-in Spark or 
use `https://archive.apache.org/dist/flink/` to download the built-in Flink release package,
but if you find it hard to reach, or the downloading speed is too slow, you can define the `spark.archive.mirror`
or `flink.archive.mirror` property to a suitable Apache mirror site. For instance,

```bash
build/mvn clean package -Dspark.archive.mirror=https://mirrors.bfsu.edu.cn/apache/spark/spark-3.0.1
```

Visit [Apache Mirrors](http://www.apache.org/mirrors/) and choose a mirror based on your region.

Specifically for developers in China mainland, you can use the pre-defined profile named `mirror-cn` 
 which use `mirrors.bfsu.edu.cn` to speed up Spark Binary downloading. For instance,

```bash
build/mvn clean package -Pmirror-cn
```
