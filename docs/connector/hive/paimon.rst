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

Apache Paimon (Incubating)
==========================

`Apache Paimon (Incubating)`_ is a streaming data lake platform that supports high-speed data ingestion, change data tracking and efficient real-time analytics.

.. tip::
   This article assumes that you have mastered the basic knowledge and operation of `Apache Paimon (Incubating)`_.
   For the knowledge about Apache Paimon (Incubating) not mentioned in this article,
   you can obtain it from its `Official Documentation`_.

By using Kyuubi, we can run SQL queries towards Apache Paimon (Incubating) which is more
convenient, easy to understand, and easy to expand than directly using
Hive to manipulate Apache Paimon (Incubating).

Apache Paimon (Incubating) Integration
--------------------------------------

To enable the integration of kyuubi hive sql engine and Apache Paimon (Incubating), you need to:

- Referencing the Apache Paimon (Incubating) :ref:`dependencies<hive-paimon-deps>`
- Setting the environment variable :ref:`configurations<hive-paimon-conf>`

.. _hive-paimon-deps:

Dependencies
************

The **classpath** of kyuubi hive sql engine with Iceberg supported consists of

1. kyuubi-hive-sql-engine-\ |release|\ _2.12.jar, the engine jar deployed with a Kyuubi distribution
2. a copy of hive distribution
3. paimon-hive-connector-<hive.binary.version>-<paimon.version>.jar (example: paimon-hive-connector-3.1-0.4-SNAPSHOT.jar), which can be found in the `Apache Paimon (Incubating) Supported Engines Hive`_

In order to make the Hive packages visible for the runtime classpath of engines, we can use one of these methods:

1. You can create an auxlib folder under the root directory of Hive, and copy paimon-hive-connector-3.1-<paimon.version>.jar into auxlib.
2. Execute ADD JAR statement in the Kyuubi to add dependencies to Hive’s auxiliary classpath. For example:

.. code-block:: sql

   ADD JAR /path/to/paimon-hive-connector-3.1-<paimon.version>.jar;

.. warning::
    The second method is not recommended. If you’re using the MR execution engine and running a join statement, you may be faced with the exception
    ``org.apache.hive.com.esotericsoftware.kryo.kryoexception: unable to find class.``

.. warning::
   Please mind the compatibility of different Apache Paimon (Incubating) and Hive versions, which can be confirmed on the page of `Apache Paimon (Incubating) multi engine support`_.

.. _hive-paimon-conf:

Configurations
**************

If you are using HDFS, make sure that the environment variable HADOOP_HOME or HADOOP_CONF_DIR is set.

Apache Paimon (Incubating) Operations
-------------------------------------

Apache Paimon (Incubating) only supports only reading table store tables through Hive.
A common scenario is to write data with Spark or Flink and read data with Hive.
You can follow this document `Apache Paimon (Incubating) Quick Start with Paimon Hive Catalog`_  to write data to a table which can also be accessed directly from Hive.
and then use Kyuubi Hive SQL engine to query the table with the following SQL ``SELECT`` statement.

Taking ``Query Data`` as an example,

.. code-block:: sql

    SELECT a, b FROM test_table ORDER BY a;

Taking ``Query External Table`` as an example,

.. code-block:: sql

    CREATE EXTERNAL TABLE external_test_table
    STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'
    LOCATION '/path/to/table/store/warehouse/default.db/test_table';

    SELECT a, b FROM test_table ORDER BY a;

.. _Apache Paimon (Incubating): https://paimon.apache.org/
.. _Official Documentation: https://paimon.apache.org/docs/master/
.. _Apache Paimon (Incubating) Quick Start with Paimon Hive Catalog: https://paimon.apache.org/docs/master/engines/hive/#quick-start-with-paimon-hive-catalog
.. _Apache Paimon (Incubating) Supported Engines Hive: https://paimon.apache.org/docs/master/engines/hive/
.. _Apache Paimon (Incubating) multi engine support: https://paimon.apache.org/docs/master/engines/overview/
