.. Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.


Getting Started with Hive JDBC
==============================

How to get the Kyuubi JDBC driver
---------------------------------

Kyuubi Thrift API is fully compatible with HiveServer2, so technically, it allows to use any Hive JDBC driver to connect
Kyuubi Server. But it's recommended to use :doc:`../client/jdbc/kyuubi_jdbc`, which is forked from
Hive 3.1.x JDBC driver, aims to support some missing functionalities of the original Hive JDBC driver.

The driver is available from Maven Central:

.. parsed-literal::

   <dependency>
       <groupId>org.apache.kyuubi</groupId>
       <artifactId>kyuubi-hive-jdbc-shaded</artifactId>
       <version>\ |release|\</version>
   </dependency>


Connect to non-kerberized Kyuubi Server
---------------------------------------

The following java code connects directly to the Kyuubi Server by JDBC without using kerberos authentication.

.. code-block:: java

   package org.apache.kyuubi.examples;

   import java.sql.*;

   public class KyuubiJDBC {

     private static String driverName = "org.apache.kyuubi.jdbc.KyuubiHiveDriver";
     private static String kyuubiJdbcUrl = "jdbc:kyuubi://localhost:10009/default;";

     public static void main(String[] args) throws SQLException {
       try (
         Connection conn = DriverManager.getConnection(kyuubiJdbcUrl);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("show databases")) {
         while (rs.next()) {
           System.out.println(rs.getString(1));
         }
       }
     }
   }

Connect to Kerberized Kyuubi Server
-----------------------------------

The following Java code uses a keytab file to login and connect to Kyuubi Server by JDBC.

.. code-block:: java

   package org.apache.kyuubi.examples;

   import java.sql.*;

   public class KyuubiJDBCDemo {

     private static String driverName = "org.apache.kyuubi.jdbc.KyuubiHiveDriver";
     private static String kyuubiJdbcUrlTemplate = "jdbc:kyuubi://localhost:10009/default;" +
             "kyuubiClientPrincipal=%s;kyuubiClientKeytab=%s;kyuubiServerPrincipal=%s";

     public static void main(String[] args) throws SQLException {
       String clientPrincipal = args[0]; // Kerberos principal
       String clientKeytab = args[1];    // Keytab file location
       String serverPrincipal = args[2]; // Kerberos principal used by Kyuubi Server
       String kyuubiJdbcUrl = String.format(kyuubiJdbcUrlTemplate, clientPrincipal, clientKeytab, serverPrincipal);
       try (
         Connection conn = DriverManager.getConnection(kyuubiJdbcUrl);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("show databases")) {
         while (rs.next()) {
           System.out.println(rs.getString(1));
         }
       }
     }
   }
