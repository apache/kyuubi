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

`Apache Paimon (Incubating)`_ is a streaming data lake platform that supports high-speed data ingestion, change data tracking, and efficient real-time analytics.

.. tip::
   This article assumes that you have mastered the basic knowledge and operation of `Apache Paimon (Incubating)`_.
   For the knowledge not mentioned in this article,
   you can obtain it from its `Official Documentation`_.

By using kyuubi, we can run SQL queries towards Apache Paimon (Incubating) which is more
convenient, easy to understand, and easy to expand than directly using flink.

Apache Paimon (Incubating) Integration
--------------------------------------

To enable the integration of kyuubi flink sql engine and Apache Paimon (Incubating), you need to:

- Referencing the Apache Paimon (Incubating) :ref:`dependencies<flink-paimon-deps>`

.. _flink-paimon-deps:

Dependencies
************

The **classpath** of kyuubi flink sql engine with Apache Paimon (Incubating) supported consists of

1. kyuubi-flink-sql-engine-\ |release|\ _2.12.jar, the engine jar deployed with a Kyuubi distribution
2. a copy of flink distribution
3. paimon-flink-<version>.jar (example: paimon-flink-1.18-0.8.1.jar), which can be found in the `Apache Paimon (Incubating) Supported Engines Flink`_
4. flink-shaded-hadoop-2-uber-<version>.jar, which code can be found in the `Pre-bundled Hadoop Jar`_

In order to make the Apache Paimon (Incubating) packages visible for the runtime classpath of engines, you need to:

1. Put the Apache Paimon (Incubating) packages into ``$FLINK_HOME/lib`` directly
2. Setting the HADOOP_CLASSPATH environment variable or copy the `Pre-bundled Hadoop Jar`_ to flink/lib.

.. warning::
   Please mind the compatibility of different Apache Paimon (Incubating) and Flink versions, which can be confirmed on the page of `Apache Paimon (Incubating) multi engine support`_.

Apache Paimon (Incubating) Operations
-------------------------------------

Taking ``CREATE CATALOG`` as a example,

.. code-block:: sql

   CREATE CATALOG my_catalog WITH (
       'type'='paimon',
       'warehouse'='file:/tmp/paimon'
   );

   USE CATALOG my_catalog;

Taking ``CREATE TABLE`` as a example,

.. code-block:: sql

   CREATE TABLE MyTable (
     user_id BIGINT,
     item_id BIGINT,
     behavior STRING,
     dt STRING,
     PRIMARY KEY (dt, user_id) NOT ENFORCED
   ) PARTITIONED BY (dt) WITH (
     'bucket' = '4'
   );

Taking ``Query Table`` as a example,

.. code-block:: sql

   SET 'execution.runtime-mode' = 'batch';
   SELECT * FROM orders WHERE catalog_id=1025;

Taking ``Streaming Query`` as a example,

.. code-block:: sql

   SET 'execution.runtime-mode' = 'streaming';
   SELECT * FROM MyTable /*+ OPTIONS ('log.scan'='latest') */;

Taking ``Rescale Bucket`` as a example,

.. code-block:: sql

   ALTER TABLE my_table SET ('bucket' = '4');
   INSERT OVERWRITE my_table PARTITION (dt = '2022-01-01');


.. _Apache Paimon (Incubating): https://paimon.apache.org/
.. _Official Documentation: https://paimon.apache.org/docs/master/
.. _Apache Paimon (Incubating) Supported Engines Flink: https://paimon.apache.org/docs/master/engines/flink/#preparing-paimon-jar-file
.. _Pre-bundled Hadoop Jar: https://flink.apache.org/downloads/#additional-components
.. _Apache Paimon (Incubating) multi engine support: https://paimon.apache.org/docs/master/engines/overview/
