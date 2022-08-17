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

`Flink Table Store`_
==========

Flink Table Store is a unified storage to build dynamic tables for both streaming and batch processing in Flink,
supporting high-speed data ingestion and timely data query.

.. tip::
   This article assumes that you have mastered the basic knowledge and operation of `Flink Table Store`_.
   For the knowledge about Flink Table Store not mentioned in this article,
   you can obtain it from its `Official Documentation`_.

By using kyuubi, we can run SQL queries towards Flink Table Store which is more
convenient, easy to understand, and easy to expand than directly using
spark to manipulate Flink Table Store.

Flink Table Store Integration
-------------------

To enable the integration of kyuubi spark sql engine and Flink Table Store through
Apache Spark Datasource V2 and Catalog APIs, you need to:

- Referencing the Flink Table Store :ref:`dependencies<spark-flink-table-store-deps>`
- Setting the spark extension and catalog :ref:`configurations<spark-flink-table-store-conf>`

.. _spark-flink-table-store-deps:

Dependencies
************

The **classpath** of kyuubi spark sql engine with Flink Table Store supported consists of

1. kyuubi-spark-sql-engine-\ |release|\ _2.12.jar, the engine jar deployed with Kyuubi distributions
2. a copy of spark distribution
3. flink-table-store-spark-<version>.jar (example: flink-table-store-spark-0.2.jar), which can be found in the `Maven Central`_

In order to make the Flink Table Store packages visible for the runtime classpath of engines, we can use one of these methods:

1. Put the Flink Table Store packages into ``$SPARK_HOME/jars`` directly
2. Set ``spark.jars=/path/to/flink-table-store-spark``

.. warning::
   Please mind the compatibility of different Flink Table Store and Spark versions, which can be confirmed on the page of `Flink Table Store multi engine support`_.

.. _spark-flink-table-store-conf:

Configurations
**************

To activate functionality of Flink Table Store, we can set the following configurations:

.. code-block:: properties

   spark.sql.catalog.tablestore=org.apache.flink.table.store.spark.SparkCatalog
   spark.sql.catalog.tablestore.warehouse=file:/tmp/warehouse

Flink Table Store Operations
------------------

Flink Table Store supports reading table store tables through Spark.
A common scenario is to write data with Flink and read data with Spark.
You can follow this document `Flink Table Store Quick Start`_  to write data to a table store table
and then use kyuubi spark sql engine to query the table with the following SQL ``SELECT`` statement.


.. code-block:: sql

   select * from table_store.default.word_count;



.. _Flink Table Store: https://nightlies.apache.org/flink/flink-table-store-docs-stable/
.. _Flink Table Store Quick Start: https://nightlies.apache.org/flink/flink-table-store-docs-stable/docs/try-table-store/quick-start/
.. _Official Documentation: https://nightlies.apache.org/flink/flink-table-store-docs-stable/
.. _Maven Central: https://mvnrepository.com/artifact/org.apache.flink
.. _Flink Table Store multi engine support: https://nightlies.apache.org/flink/flink-table-store-docs-stable/docs/engines/overview/
