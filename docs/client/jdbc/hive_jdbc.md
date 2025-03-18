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

# Hive JDBC Driver

## Instructions

Kyuubi is fully compatible with Hive JDBC and ODBC drivers that let you connect to popular Business Intelligence (BI)
tools to query, analyze and visualize data though Spark SQL engines.

It's recommended to use [Kyuubi JDBC driver](./kyuubi_jdbc.html) for new applications.

## Install Hive JDBC

For programing, the easiest way to get `hive-jdbc` is from [the maven central](https://mvnrepository.com/artifact/org.apache.hive/hive-jdbc). For example,

The following sections demonstrate how to use Hive JDBC driver 2.3.8 to connect Kyuubi Server, actually, any version
less or equals 3.1.x should work fine.

- **maven**

```xml
<dependency>
    <groupId>org.apache.hive</groupId>
    <artifactId>hive-jdbc</artifactId>
    <version>2.3.8</version>
</dependency>
```

- **sbt**

```scala
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "2.3.8"
```

- **gradle**

```groovy
implementation group: 'org.apache.hive', name: 'hive-jdbc', version: '2.3.8'
```

For BI tools, please refer to [Quick Start](../../quick_start/index.html) to check the guide for the BI tool used.
If you find there is no specific document for the BI tool that you are using, don't worry, the configuration part for all BI tools are basically the same.
Also, we will appreciate if you can help us to improve the document.

## JDBC URL

JDBC URLs have the following format:

```
jdbc:hive2://<host>:<port>/<dbName>;<sessionVars>?<kyuubiConfs>#<[spark|hive]Vars>
```

|    JDBC Parameter     |                                                                 Description                                                                  |
|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| host                  | The cluster node hosting Kyuubi Server.                                                                                                      |
| port                  | The port number to which is Kyuubi Server listening.                                                                                         |
| dbName                | Optional database name to set the current database to run the query against, use `default` if absent.                                        |
| sessionVars           | Optional `Semicolon(;)` separated `key=value` parameters for the JDBC/ODBC driver. Such as `user`, `password` and `hive.server2.proxy.user`. |
| kyuubiConfs           | Optional `Semicolon(;)` separated `key=value` parameters for Kyuubi server to create the corresponding engine, dismissed if engine exists.   |
| [spark&#124;hive]Vars | Optional `Semicolon(;)` separated `key=value` parameters for Spark/Hive variables used for variable substitution.                            |

## Example

```
jdbc:hive2://localhost:10009/default;hive.server2.proxy.user=proxy_user?kyuubi.engine.share.level=CONNECTION;spark.ui.enabled=false#var_x=y
```

