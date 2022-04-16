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

<img src="https://svn.apache.org/repos/asf/comdev/project-logos/originals/kyuubi-1.svg" alt="Kyuubi logo" width="25%" align="center" />


# Installing and Configuring Kyuubi Spark AuthZ Plugin

## Pre-install

- [Apache Ranger](https://ranger.apache.org/)

  This plugin works as a ranger rest client with Apache Ranger admin server to do privilege check.
  Thus, a ranger server need to be installed ahead and available to use.

- Building(optional)

  If your ranger admin or spark distribution is not compatible with the official pre-built [artifact](https://mvnrepository.com/artifact/org.apache.kyuubi/kyuubi-spark-authz) in maven central.
  You need to [build](build.md) the plugin targeting the spark/ranger you are using by yourself.

## Install

With the `kyuubi-spark-authz_*.jar` and its transitive dependencies available for spark runtime classpath, such as
- Copied to `$SPARK_HOME/jars`, or
- Specified to `spark.jars` configuration

## Configure

### Settings for Connecting Ranger Admin

#### ranger-spark-security.xml
- Create `ranger-spark-security.xml` in `$SPARK_HOME/conf` and add the following configurations
for pointing to the right Ranger admin server.

```xml
<configuration>
    <property>
        <name>ranger.plugin.spark.policy.rest.url</name>
        <value>ranger admin address like http://ranger-admin.org:6080</value>
    </property>

    <property>
        <name>ranger.plugin.spark.service.name</name>
        <value>a ranger hive service name</value>
    </property>

    <property>
        <name>ranger.plugin.spark.policy.cache.dir</name>
        <value>./a ranger hive service name/policycache</value>
    </property>

    <property>
        <name>ranger.plugin.spark.policy.pollIntervalMs</name>
        <value>5000</value>
    </property>

    <property>
        <name>ranger.plugin.spark.policy.source.impl</name>
        <value>org.apache.ranger.admin.client.RangerAdminRESTClient</value>
    </property>

</configuration>
```

#### ranger-spark-audit.xml

Create `ranger-spark-audit.xml` in `$SPARK_HOME/conf` and add the following configurations
to enable/disable auditing.

```xml
<configuration>

    <property>
        <name>xasecure.audit.is.enabled</name>
        <value>true</value>
    </property>

    <property>
        <name>xasecure.audit.destination.db</name>
        <value>false</value>
    </property>

    <property>
        <name>xasecure.audit.destination.db.jdbc.driver</name>
        <value>com.mysql.jdbc.Driver</value>
    </property>

    <property>
        <name>xasecure.audit.destination.db.jdbc.url</name>
        <value>jdbc:mysql://10.171.161.78/ranger</value>
    </property>

    <property>
        <name>xasecure.audit.destination.db.password</name>
        <value>rangeradmin</value>
    </property>

    <property>
        <name>xasecure.audit.destination.db.user</name>
        <value>rangeradmin</value>
    </property>

</configuration>
```

### Settings for Spark Session Extensions

Add `org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension` to the spark configuration `spark.sql.extensions`.

```properties
spark.sql.extensions=org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension
```
