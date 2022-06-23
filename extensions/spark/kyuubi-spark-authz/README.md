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

# Kyuubi Spark AuthZ Extension

## Functions

- [x] Column-level fine-grained authorization
- [x] Row-level fine-grained authorization, a.k.a. Row-level filtering
- [x] Data masking

## Build

```shell
build/mvn clean package -pl :kyuubi-spark-authz_2.12 -Dspark.version=3.2.1 -Dranger.version=2.2.0
```


### Supported Apache Spark Versions

`-Dspark.version=`

- [x] master
- [x] 3.3.x
- [x] 3.2.x (default)
- [x] 3.1.x
- [x] 3.0.x
- [x] 2.4.x
- [ ] 2.3.x and earlier

### Supported Apache Ranger Versions

`-Dranger.version=`

- [x] 2.2.x (default)
- [x] 2.1.x
- [x] 2.0.x
- [x] 1.2.x
- [x] 1.1.x
- [x] 1.0.x
- [x] 0.7.x
- [x] 0.6.x

## Usage
### Installation
Place the kyuubi-spark-authz_2.12-<version>.jar into $SPARK_HOME/jars.

### Configurations
Ranger admin client configurations
Create ranger-spark-security.xml in $SPARK_HOME/conf and add the following configurations for pointing to the right ranger admin server

```
<configuration>

    <property>
        <name>ranger.plugin.spark.policy.rest.url</name>
        <value>The ranger admin address, like http://ranger-admin.org:6080</value>
    </property>

    <property>
        <name>ranger.plugin.spark.service.name</name>
        <value>Name of the Ranger service containing policies</value>
    </property>

    <property>
        <name>ranger.plugin.spark.policy.cache.dir</name>
        <value>A ranger hive service name/policycache</value>
    </property>

    <property>
        <name>ranger.plugin.spark.policy.pollIntervalMs</name>
        <value>5000</value>
    </property>

    <property>
        <name>ranger.plugin.spark.policy.source.impl</name>
        <value>org.apache.kyuubi.plugin.spark.authz.ranger.RangerLocalClient</value>
    </property>

</configuration>
```