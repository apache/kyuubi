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

By using kyuubi, we can run SQL queries towards Apache Paimon (Incubating) which is more
convenient, easy to understand, and easy to expand than directly using
spark to manipulate Apache Paimon (Incubating).

Apache Paimon (Incubating) Integration
--------------------------------------

To enable the integration of Kyuubi Spark SQL engine and Apache Paimon (Incubating) through
Spark DataSource V2 API, you need to:

- Referencing the Apache Paimon (Incubating) :ref:`dependencies<spark-paimon-deps>`
- Setting the Spark extension and catalog :ref:`configurations<spark-paimon-conf>`

.. _spark-paimon-deps:

Dependencies
************

The **classpath** of Kyuubi Spark SQL engine with Apache Paimon (Incubating) consists of

1. kyuubi-spark-sql-engine-\ |release|\ _2.12.jar, the engine jar deployed with a Kyuubi distribution
2. a copy of Spark distribution
3. paimon-spark-<version>.jar (example: paimon-spark-3.5-0.8.1.jar), which can be found in the `Apache Paimon (Incubating) Supported Engines Spark3`_

In order to make the Apache Paimon (Incubating) packages visible for the runtime classpath of engines, we can use one of these methods:

1. Put the Apache Paimon (Incubating) packages into ``$SPARK_HOME/jars`` directly
2. Set ``spark.jars=/path/to/paimon-spark-<version>.jar``

.. warning::
   Please mind the compatibility of different Apache Paimon (Incubating) and Spark versions, which can be confirmed on the page of `Apache Paimon (Incubating) multi engine support`_.

.. _spark-paimon-conf:

Configurations
**************

To activate functionality of Apache Paimon (Incubating), we can set the following configurations:

.. code-block:: properties

   spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog
   spark.sql.catalog.paimon.warehouse=file:/tmp/paimon

Apache Paimon (Incubating) Operations
-------------------------------------


Taking ``CREATE NAMESPACE`` as a example,

.. code-block:: sql

   CREATE DATABASE paimon.default;
   USE paimon.default;

Taking ``CREATE TABLE`` as a example,

.. code-block:: sql

   create table my_table (
       k int,
       v string
   ) tblproperties (
       'primary-key' = 'k'
   );

Taking ``SELECT`` as a example,

.. code-block:: sql

   SELECT * FROM my_table;


Taking ``INSERT`` as a example,

.. code-block:: sql

   INSERT INTO my_table VALUES (1, 'Hi Again'), (3, 'Test');




.. _Apache Paimon (Incubating): https://paimon.apache.org/
.. _Official Documentation: https://paimon.apache.org/docs/master/
.. _Apache Paimon (Incubating) Supported Engines Spark3: https://paimon.apache.org/docs/master/engines/spark3/
.. _Apache Paimon (Incubating) multi engine support: https://paimon.apache.org/docs/master/engines/overview/
