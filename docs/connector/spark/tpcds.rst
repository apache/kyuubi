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

TPC-DS
======

The TPC-DS is a decision support benchmark. It consists of a suite of business oriented ad-hoc queries and concurrent
data modifications. The queries and the data populating the database have been chosen to have broad industry-wide
relevance.

.. tip::
   This article assumes that you have mastered the basic knowledge and operation of `TPC-DS`_.
   For the knowledge about TPC-DS not mentioned in this article, you can obtain it from its `Official Documentation`_.

This connector can be used to test the capabilities and query syntax of Spark without configuring access to an external
data source. When you query a TPC-DS table, the connector generates the data on the fly using a deterministic algorithm.

Goto `Try Kyuubi`_ to explore TPC-DS data instantly!

TPC-DS Integration
------------------

To enable the integration of Kyuubi Spark SQL engine and TPC-DS through
Spark DataSource V2 API, you need to:

- Referencing the TPC-DS connector :ref:`dependencies<spark-tpcds-deps>`
- Setting the Spark catalog :ref:`configurations<spark-tpcds-conf>`

.. _spark-tpcds-deps:

Dependencies
************

The **classpath** of Kyuubi Spark SQL engine with TPC-DS supported consists of

1. kyuubi-spark-sql-engine-\ |release|\ _2.12.jar, the engine jar deployed with a Kyuubi distribution
2. a copy of Spark distribution
3. kyuubi-spark-connector-tpcds-\ |release|\ _2.12.jar, which can be found in the `Maven Central`_

In order to make the TPC-DS connector package visible for the runtime classpath of engines, we can use one of these methods:

1. Put the TPC-DS connector package into ``$SPARK_HOME/jars`` directly
2. Set spark.jars=kyuubi-spark-connector-tpcds-\ |release|\ _2.12.jar

.. _spark-tpcds-conf:

Configurations
**************

To add TPC-DS tables as a catalog, we can set the following configurations in ``$SPARK_HOME/conf/spark-defaults.conf``:

.. code-block:: properties

   # (required) Register a catalog named `tpcds` for the spark engine.
   spark.sql.catalog.tpcds=org.apache.kyuubi.spark.connector.tpcds.TPCDSCatalog

   # (optional) Excluded database list from the catalog, all available databases are:
   #            sf0, tiny, sf1, sf10, sf30, sf100, sf300, sf1000, sf3000, sf10000, sf30000, sf100000.
   spark.sql.catalog.tpcds.excludeDatabases=sf10000,sf30000

   # (optional) When true, use CHAR/VARCHAR, otherwise use STRING. It affects output of the table schema,
   #            e.g. `SHOW CREATE TABLE <table>`, `DESC <table>`.
   spark.sql.catalog.tpcds.useAnsiStringType=false

   # (optional) TPCDS changed table schemas in v2.6.0, turn off this option to use old table schemas.
   #            See detail at: https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-ds_v3.2.0.pdf
   spark.sql.catalog.tpcds.useTableSchema_2_6=true

   # (optional) Maximum bytes per task, consider reducing it if you want higher parallelism.
   spark.sql.catalog.tpcds.read.maxPartitionBytes=128m

TPC-DS Operations
-----------------

Listing databases under `tpcds` catalog.

.. code-block:: sql

   SHOW DATABASES IN tpcds;

Listing tables under `tpcds.sf1` database.

.. code-block:: sql

   SHOW TABLES IN tpcds.sf1;

Switch current database to `tpcds.sf1` and run a query against it.

.. code-block:: sql

   USE tpcds.sf1;
   SELECT * FROM store;

.. _Official Documentation: https://www.tpc.org/tpcds/
.. _Try Kyuubi: https://try.kyuubi.cloud/
.. _Maven Central: https://repo1.maven.org/maven2/org/apache/kyuubi/kyuubi-spark-connector-tpcds_2.12/
