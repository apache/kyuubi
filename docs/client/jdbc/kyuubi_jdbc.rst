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
   Kyuubi community maintains a forked Hive JDBC driver module and provides both shaded and non-shaded packages.

This packages aims to support some missing functionalities of the original Hive JDBC driver.
For Kyuubi engines that support multiple catalogs, it provides meta APIs for better support.
The behaviors of the original Hive JDBC driver have remained.

To access a Hive data warehouse or new Lakehouse formats, such as Apache Iceberg/Hudi, Delta Lake using the Kyuubi JDBC driver
for Apache kyuubi, you need to configure the following:

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

.. parsed-literal::

   <dependency>
       <groupId>org.apache.kyuubi</groupId>
       <artifactId>kyuubi-hive-jdbc-shaded</artifactId>
       <version>\ |release|\</version>
   </dependency>

sbt
^^^

.. parsed-literal::

   libraryDependencies += "org.apache.kyuubi" % "kyuubi-hive-jdbc-shaded" % "\ |release|\"


Gradle
^^^^^^

.. parsed-literal::

   implementation group: 'org.apache.kyuubi', name: 'kyuubi-hive-jdbc-shaded', version: '\ |release|\'

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

   private static Connection newKyuubiConnection() throws Exception {
     Connection connection = DriverManager.getConnection(CONNECTION_URL);
     return connection;
   }

.. _building_url:

Building the Connection URL
---------------------------

Basic Connection URL format
***************************

Use the connection URL to supply connection information to the kyuubi server or cluster that you are
accessing. The following is the format of the connection URL for the Kyuubi Hive JDBC Driver

.. code-block::

   jdbc:subprotocol://host:port[/catalog]/[schema];<clientProperties;><[#|?]sessionProperties>

- subprotocol: kyuubi or hive2
- host: DNS or IP address of the kyuubi server
- port: The number of the TCP port that the server uses to listen for client requests
- catalog: Optional catalog name to set the current catalog to run the query against.
- schema: Optional database name to set the current database to run the query against, use `default` if absent.
- clientProperties: Optional `semicolon(;)` separated `key=value` parameters identified and affect the client behavior locally. e.g., user=foo;password=bar.
- sessionProperties: Optional `semicolon(;)` separated `key=value` parameters used to configure the session, operation or background engines.
  For instance, `kyuubi.engine.share.level=CONNECTION` determines the background engine instance is used only by the current connection. `spark.ui.enabled=false` disables the Spark UI of the engine.

.. important::
   - The sessionProperties MUST come after a leading number sign(#) or question mark (?).
   - Properties are case-sensitive
   - Do not duplicate properties in the connection URL

Connection URL over HTTP
************************

.. versionadded:: 1.6.0

.. code-block::

   jdbc:subprotocol://host:port/schema;transportMode=http;httpPath=<http_endpoint>

- http_endpoint is the corresponding HTTP endpoint configured by `kyuubi.frontend.thrift.http.path` at the server side.

Connection URL over Service Discovery
*************************************

.. code-block::

   jdbc:subprotocol://<zookeeper quorum>/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=kyuubi

- zookeeper quorum is the corresponding zookeeper cluster configured by `kyuubi.ha.addresses` at the server side.
- zooKeeperNamespace is  the corresponding namespace configured by `kyuubi.ha.namespace` at the server side.

HiveServer2 Compatibility
*************************

.. versionadded:: 1.8.0

JDBC Drivers need to negotiate a protocol version with Kyuubi Server/HiveServer2 when connecting.

Kyuubi Hive JDBC Driver offers protocol version v10 (`clientProtocolVersion=9`, supported since Hive 2.3.0)
to server by default.

If you need to connect to HiveServer2 before 2.3.0,
please set client property `clientProtocolVersion` to a lower number.

.. code-block::

   jdbc:subprotocol://host:port[/catalog]/[schema];clientProtocolVersion=9;


.. tip::
    All supported protocol versions and corresponding Hive versions can be found in `TProtocolVersion.java`_
    and its git commits.

Kerberos Authentication
-----------------------
Since 1.6.0, Kyuubi JDBC driver implements the Kerberos authentication based on JAAS framework instead of `Hadoop UserGroupInformation`_,
which means it does not forcibly rely on Hadoop dependencies to connect a kerberized Kyuubi Server.

Kyuubi JDBC driver supports different approaches to connect a kerberized Kyuubi Server. First of all, please follow
the `krb5.conf instruction`_ to setup ``krb5.conf`` properly.

Authentication by Principal and Keytab
**************************************

.. versionadded:: 1.6.0

.. tip::

   It's the simplest way w/ minimal setup requirements for Kerberos authentication.

It's straightforward to use principal and keytab for Kerberos authentication, just simply configure them in the JDBC URL.

.. code-block::

   jdbc:kyuubi://host:port/schema;kyuubiClientPrincipal=<clientPrincipal>;kyuubiClientKeytab=<clientKeytab>;kyuubiServerPrincipal=<serverPrincipal>

- kyuubiClientPrincipal: Kerberos ``principal`` for client authentication
- kyuubiClientKeytab: path of Kerberos ``keytab`` file for client authentication
- kyuubiClientTicketCache: path of Kerberos ``ticketCache`` file for client authentication, available since 1.8.0.
- kyuubiServerPrincipal: Kerberos ``principal`` configured by `kyuubi.kinit.principal` at the server side. ``kyuubiServerPrincipal`` is available
  as an alias of ``principal`` since 1.7.0, use ``principal`` for previous versions.

Authentication by Principal and TGT Cache
*****************************************

Another typical usage of Kerberos authentication is using `kinit` to generate the TGT cache first, then the application
does Kerberos authentication through the TGT cache.

.. code-block::

   jdbc:kyuubi://host:port/schema;kyuubiServerPrincipal=<serverPrincipal>

Authentication by `Hadoop UserGroupInformation`_ ``doAs`` (programing only)
***************************************************************************

.. tip::

  This approach allows project which already uses `Hadoop UserGroupInformation`_ for Kerberos authentication to easily
  connect the kerberized Kyuubi Server. This approach does not work between [1.6.0, 1.7.0], and got fixed in 1.7.1.

.. code-block::

  String jdbcUrl = "jdbc:kyuubi://host:port/schema;kyuubiServerPrincipal=<serverPrincipal>"
  UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytab(clientPrincipal, clientKeytab);
  ugi.doAs((PrivilegedExceptionAction<String>) () -> {
    Connection conn = DriverManager.getConnection(jdbcUrl);
    ...
  });

Authentication by Subject (programing only)
*******************************************

.. code-block:: java

   String jdbcUrl = "jdbc:kyuubi://host:port/schema;kyuubiServerPrincipal=<serverPrincipal>;kerberosAuthType=fromSubject"
   Subject kerberizedSubject = ...;
   Subject.doAs(kerberizedSubject, (PrivilegedExceptionAction<String>) () -> {
     Connection conn = DriverManager.getConnection(jdbcUrl);
     ...
   });

.. _Maven Central: https://mvnrepository.com/artifact/org.apache.kyuubi/kyuubi-hive-jdbc-shaded
.. _JDBC Applications: ../bi_tools/index.html
.. _java.sql.DriverManager: https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html
.. _Hadoop UserGroupInformation: https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/security/UserGroupInformation.html
.. _krb5.conf instruction: https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/KerberosReq.html
.. _TProtocolVersion.java: https://github.com/apache/hive/blob/master/service-rpc/src/gen/thrift/gen-javabean/org/apache/hive/service/rpc/thrift/TProtocolVersion.java