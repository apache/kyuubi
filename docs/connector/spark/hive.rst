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

`Hive`_
==========

The Kyuubi Hive Connector is a datasource for both reading and writing Hive table,
It is implemented based on Spark DataSource V2, and supports concatenating multiple Hive metastore at the same time.

This connector can be used to federate queries of multiple hives warehouse in a single Spark cluster.

Hive Connector Integration
-------------------

To enable the integration of kyuubi spark sql engine and Hive connector through
Apache Spark Datasource V2 and Catalog APIs, you need to:

- Referencing the Hive connector :ref:`dependencies<kyuubi-hive-deps>`
- Setting the spark extension and catalog :ref:`configurations<kyuubi-hive-conf>`

.. _kyuubi-hive-deps:

Dependencies
************

The **classpath** of kyuubi spark sql engine with Hive connector supported consists of

1. kyuubi-spark-connector-hive_2.12-\ |release|\ , the hive connector jar deployed with Kyuubi distributions
2. a copy of spark distribution

In order to make the Hive connector packages visible for the runtime classpath of engines, we can use one of these methods:

1. Put the Kyuubi Hive connector packages into ``$SPARK_HOME/jars`` directly
2. Set ``spark.jars=/path/to/kyuubi-hive-connector``

.. _kyuubi-hive-conf:

Configurations
**************

To activate functionality of Kyuubi Hive connector, we can set the following configurations:

.. code-block:: properties

   spark.sql.catalog.hive_catalog     org.apache.kyuubi.spark.connector.hive.HiveTableCatalog
   spark.sql.catalog.hive_catalog.spark.sql.hive.metastore.version     hive-metastore-version
   spark.sql.catalog.hive_catalog.hive.metastore.uris     thrift://metastore-host:port
   spark.sql.catalog.hive_catalog.hive.metastore.port     port
   spark.sql.catalog.hive_catalog.spark.sql.hive.metastore.jars     path
   spark.sql.catalog.hive_catalog.spark.sql.hive.metastore.jars.path     file:///opt/hive1/lib/*.jar

.. tip::
   For details about the multi-version Hive configuration, see the related multi-version Hive configurations supported by Apache Spark.

Hive Connector Operations
------------------

Taking ``CREATE NAMESPACE`` as a example,

.. code-block:: sql

   CREATE NAMESPACE ns;

Taking ``CREATE TABLE`` as a example,

.. code-block:: sql

   CREATE TABLE hive_catalog.ns.foo (
     id bigint COMMENT 'unique id',
     data string)
   USING parquet;

Taking ``SELECT`` as a example,

.. code-block:: sql

   SELECT * FROM hive_catalog.ns.foo;

Taking ``INSERT`` as a example,

.. code-block:: sql

   INSERT INTO hive_catalog.ns.foo VALUES (1, 'a'), (2, 'b'), (3, 'c');

Taking ``DROP TABLE`` as a example,

.. code-block:: sql

   DROP TABLE hive_catalog.ns.foo;

Taking ``DROP NAMESPACE`` as a example,

.. code-block:: sql

   DROP NAMESPACE hive_catalog.ns;

.. _Apache Spark: https://spark.apache.org/