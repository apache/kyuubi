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

By using Kyuubi, we can run SQL queries towards Flink Table Store which is more
convenient, easy to understand, and easy to expand than directly using
Hive to manipulate Flink Table Store.

Flink Table Store Integration
-------------------

To enable the integration of kyuubi flink sql engine and Flink Table Store, you need to:

- Referencing the Flink Table Store :ref:`dependencies<hive-flink-table-store-deps>`
- Setting the environment variable :ref:`configurations<hive-flink-table-store-conf>`

.. _hive-flink-table-store-deps:

Dependencies
************

The **classpath** of kyuubi hive sql engine with Iceberg supported consists of

1. kyuubi-hive-sql-engine-\ |release|\ _2.12.jar, the engine jar deployed with Kyuubi distributions
2. a copy of hive distribution
3. flink-table-store-hive-connector-<flink-table-store.version>_<hive.binary.version>.jar (example: flink-table-store-hive-connector-0.4.0_3.1.jar), which can be found in the `Installation Table Store in Hive`_

In order to make the Hive packages visible for the runtime classpath of engines, we can use one of these methods:

1. You can create an auxlib folder under the root directory of Hive, and copy flink-table-store-hive-connector-0.4.0_3.1.jar into auxlib.
2. Execute ADD JAR statement in the Kyuubi to add dependencies to Hive’s auxiliary classpath. For example:

.. code-block:: sql

   ADD JAR /path/to/flink-table-store-hive-connector-0.4.0_3.1.jar;

.. warning::
    The second method is not recommended. If you’re using the MR execution engine and running a join statement, you may be faced with the exception
    ``org.apache.hive.com.esotericsoftware.kryo.kryoexception: unable to find class.``

.. warning::
   Please mind the compatibility of different Flink Table Store and Hive versions, which can be confirmed on the page of `Flink Table Store multi engine support`_.

.. _hive-flink-table-store-conf:

Configurations
**************

If you are using HDFS, make sure that the environment variable HADOOP_HOME or HADOOP_CONF_DIR is set.

Flink Table Store  Operations
------------------

Flink Table Store only supports only reading table store tables through Hive.
A common scenario is to write data with Flink and read data with Hive.
You can follow this document `Flink Table Store Quick Start`_  to write data to a table store table
and then use Kyuubi Hive SQL engine to query the table with the following SQL ``SELECT`` statement.

Taking ``Query Data`` as an example,

.. code-block:: sql

    SELECT a, b FROM test_table ORDER BY a;

Taking ``Query External Table`` as an example,

.. code-block:: sql

    CREATE EXTERNAL TABLE external_test_table
    STORED BY 'org.apache.flink.table.store.hive.TableStoreHiveStorageHandler'
    LOCATION '/path/to/table/store/warehouse/default.db/test_table';

    SELECT a, b FROM test_table ORDER BY a;

.. _Flink Table Store: https://nightlies.apache.org/flink/flink-table-store-docs-stable/
.. _Flink Table Store Quick Start: https://nightlies.apache.org/flink/flink-table-store-docs-stable/docs/try-table-store/quick-start/
.. _Official Documentation: https://nightlies.apache.org/flink/flink-table-store-docs-release-0.4/docs/engines/hive/
.. _Installation Table Store in Hive: https://nightlies.apache.org/flink/flink-table-store-docs-release-0.4/docs/engines/hive/#installation
.. _Flink Table Store multi engine support: https://nightlies.apache.org/flink/flink-table-store-docs-stable/docs/engines/overview/
