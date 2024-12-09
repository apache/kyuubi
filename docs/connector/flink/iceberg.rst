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
flink to manipulate Iceberg.

Iceberg Integration
-------------------

To enable the integration of kyuubi flink sql engine and Iceberg through Catalog APIs, you need to:

- Referencing the Iceberg :ref:`dependencies<flink-iceberg-deps>`

.. _flink-iceberg-deps:

Dependencies
************

The **classpath** of kyuubi flink sql engine with Iceberg supported consists of

1. kyuubi-flink-sql-engine-\ |release|\ _2.12.jar, the engine jar deployed with a Kyuubi distribution
2. a copy of flink distribution
3. iceberg-flink-runtime-<flink.version>-<iceberg.version>.jar (example: iceberg-flink-runtime-1.18-1.7.0.jar), which can be found in the `Maven Central`_

In order to make the Iceberg packages visible for the runtime classpath of engines, we can use one of these methods:

1. Put the Iceberg packages into ``$FLINK_HOME/lib`` directly
2. Set ``pipeline.jars=/path/to/iceberg-flink-runtime``

.. warning::
   Please mind the compatibility of different Iceberg and Flink versions, which can be confirmed on the page of `Iceberg multi engine support`_.

Iceberg Operations
------------------

Taking ``CREATE CATALOG`` as a example,

.. code-block:: sql

   CREATE CATALOG hive_catalog WITH (
     'type'='iceberg',
     'catalog-type'='hive',
     'uri'='thrift://localhost:9083',
     'warehouse'='hdfs://nn:8020/warehouse/path'
   );
   USE CATALOG hive_catalog;

Taking ``CREATE DATABASE`` as a example,

.. code-block:: sql

   CREATE DATABASE iceberg_db;
   USE iceberg_db;

Taking ``CREATE TABLE`` as a example,

.. code-block:: sql

   CREATE TABLE `hive_catalog`.`default`.`sample` (
     id BIGINT COMMENT 'unique id',
     data STRING
   );

Taking ``Batch Read`` as a example,

.. code-block:: sql

   SET execution.runtime-mode = batch;
   SELECT * FROM sample;

Taking ``Streaming Read`` as a example,

.. code-block:: sql

   SET execution.runtime-mode = streaming;
   SELECT * FROM sample /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/ ;

Taking ``INSERT INTO`` as a example,

.. code-block:: sql

   INSERT INTO `hive_catalog`.`default`.`sample` VALUES (1, 'a');
   INSERT INTO `hive_catalog`.`default`.`sample` SELECT id, data from other_kafka_table;

Taking ``INSERT OVERWRITE`` as a example,
Flink streaming job does not support INSERT OVERWRITE.

.. code-block:: sql

   INSERT OVERWRITE `hive_catalog`.`default`.`sample` VALUES (1, 'a');
   INSERT OVERWRITE `hive_catalog`.`default`.`sample` PARTITION(data='a') SELECT 6;

.. _Iceberg: https://iceberg.apache.org/
.. _Official Documentation: https://iceberg.apache.org/docs/latest/
.. _Maven Central: https://mvnrepository.com/artifact/org.apache.iceberg
.. _Iceberg multi engine support: https://iceberg.apache.org/multi-engine-support/
