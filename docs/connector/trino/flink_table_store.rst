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
trino to manipulate Flink Table Store.

Flink Table Store Integration
-------------------

To enable the integration of kyuubi trino sql engine and Flink Table Store, you need to:

- Referencing the Flink Table Store :ref:`dependencies<trino-flink-table-store-deps>`
- Setting the trino extension and catalog :ref:`configurations<trino-flink-table-store-conf>`

.. _trino-flink-table-store-deps:

Dependencies
************

The **classpath** of kyuubi trino sql engine with Flink Table Store supported consists of

1. kyuubi-trino-sql-engine-\ |release|\ _2.12.jar, the engine jar deployed with Kyuubi distributions
2. a copy of trino distribution
3. flink-table-store-trino-<version>.jar (example: flink-table-store-trino-0.2.jar), which code can be found in the `Source Code`_
4. flink-shaded-hadoop-2-uber-2.8.3-10.0.jar, which code can be found in the `Pre-bundled Hadoop 2.8.3`_

In order to make the Flink Table Store packages visible for the runtime classpath of engines, we can use these methods:

1. Build the flink-table-store-trino-<version>.jar by reference to `Flink Table Store Trino README`_
2. Put the flink-table-store-trino-<version>.jar and flink-shaded-hadoop-2-uber-2.8.3-10.0.jar packages into ``$TRINO_SERVER_HOME/plugin/tablestore`` directly

.. warning::
   Please mind the compatibility of different Flink Table Store and Trino versions, which can be confirmed on the page of `Flink Table Store multi engine support`_.

.. _trino-flink-table-store-conf:

Configurations
**************

To activate functionality of Flink Table Store, we can set the following configurations:

Catalogs are registered by creating a catalog properties file in the $TRINO_SERVER_HOME/etc/catalog directory.
For example, create $TRINO_SERVER_HOME/etc/catalog/tablestore.properties with the following contents to mount the tablestore connector as the tablestore catalog:

.. code-block:: properties

   connector.name=tablestore
   warehouse=file:///tmp/warehouse

Flink Table Store Operations
------------------

Flink Table Store supports reading table store tables through Trino.
A common scenario is to write data with Flink and read data with Trino.
You can follow this document `Flink Table Store Quick Start`_  to write data to a table store table
and then use kyuubi trino sql engine to query the table with the following SQL ``SELECT`` statement.


.. code-block:: sql

   SELECT * FROM tablestore.default.t1


.. _Flink Table Store: https://nightlies.apache.org/flink/flink-table-store-docs-stable/
.. _Flink Table Store Quick Start: https://nightlies.apache.org/flink/flink-table-store-docs-stable/docs/try-table-store/quick-start/
.. _Official Documentation: https://nightlies.apache.org/flink/flink-table-store-docs-stable/
.. _Source Code: https://github.com/JingsongLi/flink-table-store-trino
.. _Flink Table Store multi engine support: https://nightlies.apache.org/flink/flink-table-store-docs-stable/docs/engines/overview/
.. _Pre-bundled Hadoop 2.8.3: https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
.. _Flink Table Store Trino README: https://github.com/JingsongLi/flink-table-store-trino#readme
