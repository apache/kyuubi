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

`Iceberg`_
==========

Apache Iceberg is an open table format for huge analytic datasets.
Iceberg adds tables to compute engines including Spark, Trino, PrestoDB, Flink, Hive and Impala
using a high-performance table format that works just like a SQL table.

.. tip::
   This article assumes that you have mastered the basic knowledge and operation of `Iceberg`_.
   For the knowledge about Iceberg not mentioned in this article,
   you can obtain it from its `Official Documentation`_.

By using kyuubi, we can run SQL queries towards Iceberg which is more
convenient, easy to understand, and easy to expand than directly using
spark to manipulate Iceberg.

Iceberg Integration
-------------------

To enable the integration of Kyuubi Spark SQL engine and Iceberg through
Spark DataSource V2 API, you need to:

- Referencing the Iceberg :ref:`dependencies<spark-iceberg-deps>`
- Setting the Spark extension and catalog :ref:`configurations<spark-iceberg-conf>`

.. _spark-iceberg-deps:

Dependencies
************

The **classpath** of Kyuubi Spark SQL engine with Iceberg supported consists of

1. kyuubi-spark-sql-engine-\ |release|\ _2.12.jar, the engine jar deployed with a Kyuubi distribution
2. a copy of Spark distribution
3. iceberg-spark-runtime-<spark.version>_<scala.version>-<iceberg.version>.jar (example: iceberg-spark-runtime-3.5_2.12-1.7.0.jar), which can be found in the `Maven Central`_

In order to make the Iceberg packages visible for the runtime classpath of engines, we can use one of these methods:

1. Put the Iceberg packages into ``$SPARK_HOME/jars`` directly
2. Set ``spark.jars=/path/to/iceberg-spark-runtime``

.. warning::
   Please mind the compatibility of different Iceberg and Spark versions, which can be confirmed on the page of `Iceberg multi engine support`_.

.. _spark-iceberg-conf:

Configurations
**************

To activate functionality of Iceberg, we can set the following configurations:

.. code-block:: properties

   spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog
   spark.sql.catalog.spark_catalog.type=hive
   spark.sql.catalog.spark_catalog.uri=thrift://metastore-host:port
   spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

Iceberg Operations
------------------

Taking ``CREATE TABLE`` as a example,

.. code-block:: sql

   CREATE TABLE foo (
     id bigint COMMENT 'unique id',
     data string)
   USING iceberg;

Taking ``SELECT`` as a example,

.. code-block:: sql

   SELECT * FROM foo;

Taking ``INSERT`` as a example,

.. code-block:: sql

   INSERT INTO foo VALUES (1, 'a'), (2, 'b'), (3, 'c');

Taking ``UPDATE`` as a example, Spark 3.1 added support for UPDATE queries that update matching rows in tables.

.. code-block:: sql

   UPDATE foo SET data = 'd', id = 4 WHERE id >= 3 and id < 4;

Taking ``DELETE FROM`` as a example, Spark 3 added support for DELETE FROM queries to remove data from tables.

.. code-block:: sql

   DELETE FROM foo WHERE id >= 1 and id < 2;

Taking ``MERGE INTO`` as a example,

.. code-block:: sql

   MERGE INTO target_table t
   USING source_table s
   ON t.id = s.id
   WHEN MATCHED AND s.opType = 'delete' THEN DELETE
   WHEN MATCHED AND s.opType = 'update' THEN UPDATE SET id = s.id, data = s.data
   WHEN NOT MATCHED AND s.opType = 'insert' THEN INSERT (id, data) VALUES (s.id, s.data);

.. _Iceberg: https://iceberg.apache.org/
.. _Official Documentation: https://iceberg.apache.org/docs/latest/
.. _Maven Central: https://mvnrepository.com/artifact/org.apache.iceberg
.. _Iceberg multi engine support: https://iceberg.apache.org/multi-engine-support/
