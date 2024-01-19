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

# Trino JDBC Driver

## Instructions

Kyuubi currently supports the Trino connection protocol, so we can use Trino-JDBC to connect to the kyuubi server
and submit SQL to Spark, Trino and other engines for execution.

## Start Kyuubi Trino Server

First we should configure the trino protocol and the service port in the `kyuubi.conf`

```
kyuubi.frontend.protocols TRINO
kyuubi.frontend.trino.bind.port 10999 #default port
```

## Install Trino JDBC

Download [trino-jdbc-411.jar](https://repo1.maven.org/maven2/io/trino/trino-jdbc/411/trino-jdbc-363.jar) and add it to the classpath of your Java application.

The driver is also available from Maven Central:

```xml
<dependency>
    <groupId>io.trino</groupId>
    <artifactId>trino-jdbc</artifactId>
    <version>411</version>
</dependency>
```

## JDBC URL

When your driver is loaded, registered and configured, you are ready to connect to Trino from your application. The following JDBC URL formats are supported:

```
jdbc:trino://host:port
```

Trino JDBC example

```java
String trinoHost = "localhost";
String trinoPort = "10999";
String trinoUser = "default";
String trinoPassword = null;
Connection connection = null;
ResultSet rs = null;

try {
    // Create the connection using the JDBC URL
    connection = DriverManager.getConnection("jdbc:trino://" + trinoHost + ":" + trinoPort, trinoUser, trinoPassword);

    // Do whatever you need to do with the connection
    Statement stmt = connection.createStatement();
    rs = stmt.executeQuery("SELECT 1");

    while (rs.next()) {
    // retrieve data from the ResultSet
    }

} catch (Exception e) {
    e.printStackTrace();
} finally {
    try {
        // Close the connection when you're done with it
        if (rs != null) rs.close();
        if (connection != null) connection.close();
    } catch (Exception e) {
        e.printStackTrace();
    }
} 
```

The configuration of the connection parameters can be found in the official trino documentation at: https://trino.io/docs/current/client/jdbc.html#connection-parameters

