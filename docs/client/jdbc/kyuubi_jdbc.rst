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

Kyuubi Hive JDBC Driver
=======================

.. versionadded:: 1.4.0
   Since 1.4.0, kyuubi community maintains a forked hive jdbc driver module and provides both shaded and non-shaded packages.

This packages aims to support some missing functionalities of the original hive jdbc.
For kyuubi engines that support multiple catalogs, it provides meta APIs for better support.
The behaviors of the original hive jdbc have remained.

To access a Hive data warehouse or new lakehouse formats, such as Apache Iceberg/Hudi, delta lake using the kyuubi jdbc driver for Apache kyuubi, you need to configure
the following:

- The list of driver library files - :ref:`referencing-libraries`.
- The Driver or DataSource class - :ref:`registering_class`.
- The connection URL for the driver - :ref:`building_url`

.. _referencing-libraries:

Referencing the JDBC Driver Libraries
-------------------------------------

Before you use the jdbc driver for Apache Kyuubi, the JDBC application or Java code that
you are using to connect to your data must be able to access the driver JAR files.

Using the Driver in Java Code
*****************************

In the code, specify the artifact `kyuubi-hive-jdbc-shaded` from `Maven Central`_ according to the build tool you use.

Maven
^^^^^

.. code-block:: xml

   <dependency>
       <groupId>org.apache.kyuubi</groupId>
       <artifactId>kyuubi-hive-jdbc-shaded</artifactId>
       <version>1.5.2-incubating</version>
   </dependency>

Sbt
^^^

.. code-block:: sbt

   libraryDependencies += "org.apache.kyuubi" % "kyuubi-hive-jdbc-shaded" % "1.5.2-incubating"


Gradle
^^^^^^

.. code-block:: gradle

   implementation group: 'org.apache.kyuubi', name: 'kyuubi-hive-jdbc-shaded', version: '1.5.2-incubating'

Using the Driver in a JDBC Application
**************************************

For `JDBC Applications`_, such as BI tools, SQL IDEs, please check the specific guide for detailed information.

.. note:: Is your favorite tool missing?
   `Report an feature request <https://kyuubi.apache.org/issue_tracking.html>`_ or help us document it.

.. _registering_class:

Registering the Driver Class
----------------------------

Before connecting to your data, you must register the JDBC Driver class for your application.

- org.apache.kyuubi.jdbc.KyuubiHiveDriver
- org.apache.kyuubi.jdbc.KyuubiDriver (Deprecated)

The following sample code shows how to use the `java.sql.DriverManager`_ class to establish a
connection for JDBC:

.. code-block:: java

   private static Connection connectViaDM() throws Exception
   {
      Connection connection = null;
      connection = DriverManager.getConnection(CONNECTION_URL);
      return connection;
   }

.. _building_url:

Building the Connection URL
---------------------------

Basic Connection URL format
***************************

Use the connection URL to supply connection information to the kyuubi server or cluster that you are
accessing. The following is the format of the connection URL for the Kyuubi Hive JDBC Driver

.. code-block:: jdbc

   jdbc:subprotocol://host:port/schema;<clientProperties;><[#|?]sessionProperties>

- subprotocol: kyuubi or hive2
- host: DNS or IP address of the kyuubi server
- port: The number of the TCP port that the server uses to listen for client requests
- dbName: Optional database name to set the current database to run the query against, use `default` if absent.
- clientProperties: Optional `semicolon(;)` separated `key=value` parameters identified and affect the client behavior locally. e.g., user=foo;password=bar.
- sessionProperties: Optional `semicolon(;)` separated `key=value` parameters used to configure the session, operation or background engines.
  For instance, `kyuubi.engine.share.level=CONNECTION` determines the background engine instance is used only by the current connection. `spark.ui.enabled=false` disables the Spark UI of the engine.

.. important::
   - The sessionProperties MUST come after a leading number sign(#) or question mark (?).
   - Properties are case-sensitive
   - Do not duplicate properties in the connection URL

Connection URL over Http
************************

.. versionadded:: 1.6.0

.. code-block:: jdbc

   jdbc:subprotocol://host:port/schema;transportMode=http;httpPath=<http_endpoint>

- http_endpoint is the corresponding HTTP endpoint configured by `kyuubi.frontend.thrift.http.path` at the server side.

Connection URL over Service Discovery
*************************************

.. code-block:: jdbc

   jdbc:subprotocol://<zookeeper quorum>/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=kyuubi

- zookeeper quorum is the corresponding zookeeper cluster configured by `kyuubi.ha.zookeeper.quorum` at the server side.
- zooKeeperNamespace is  the corresponding namespace configured by `kyuubi.ha.zookeeper.namespace` at the server side.

Authentication
--------------


DataTypes
---------

.. _Maven Central: https://mvnrepository.com/artifact/org.apache.kyuubi/kyuubi-hive-jdbc-shaded
.. _JDBC Applications: ../bi_tools/index.html
.. _java.sql.DriverManager: https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html
