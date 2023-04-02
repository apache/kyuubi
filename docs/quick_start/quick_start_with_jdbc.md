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

# Getting Started with JDBC

## How to get Kyuubi JDBC driver

Kyuubi Thrift API is fully compatible w/ HiveServer2, so technically, it allows to use any Hive JDBC driver to connect
Kyuubi Server. But it's recommended to use [Kyuubi Hive JDBC driver](../client/jdbc/kyuubi_jdbc), which is fork of
Hive 3.1.x JDBC driver, with some enhancements to support Kyuubi features.

The driver is available from Maven Central:

{{ kyuubi_hive_jdbc_shaded_maven_ref }}

## Connect to non-Kerberized Kyuubi Server

```java
package org.apache.kyuubi.examples;
  
import java.sql.*;
 
public class KyuubiJDBC {
 
    private static String driverName = "org.apache.kyuubi.jdbc.KyuubiHiveDriver";
    private static String kyuubiJdbcUrl = "jdbc:kyuubi://localhost:10009/default;";
 
    public static void main(String[] args) throws SQLException {
        Connection conn = DriverManager.getConnection(kyuubiJdbcUrl);
        Statement stmt = conn.createStatement();
        ResultSet rs = st.executeQuery("show databases");
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        rs.close();
        stmt.close();
        conn.close();
    }
}
```

## Connect to Kerberized Kyuubi Server

The following Java code is using a keytab file to login and connect to Kyuubi Server by JDBC.

```java
package org.apache.kyuubi.examples;

import java.sql.*;

public class KyuubiJDBCDemo {

    private static String driverName = "org.apache.kyuubi.jdbc.KyuubiHiveDriver";
    private static String kyuubiJdbcUrlTemplate = "jdbc:kyuubi://localhost:10009/default;" +
            "clientPrincipal=%s;clientKeytab=%s;serverPrincipal=%s";

    public static void main(String[] args) throws SQLException {
        String clientPrincipal = args[0]; // Kerberos principal
        String clientKeytab = args[1];    // Keytab file location
        String serverPrincipal = arg[2];  // Kerberos principal used by Kyuubi Server

        String kyuubiJdbcUrl = String.format(kyuubiJdbcUrl, clientPrincipal, clientKeytab, serverPrincipal);
        Connection conn = DriverManager.getConnection(kyuubiJdbcUrl);
        Statement stmt = conn.createStatement();
        ResultSet rs = st.executeQuery("show databases");
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        rs.close();
        stmt.close();
        conn.close();
    }
}
```

