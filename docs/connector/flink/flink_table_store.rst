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
flink to manipulate Flink Table Store.

Flink Table Store Integration
-------------------

To enable the integration of kyuubi flink sql engine and Flink Table Store, you need to:

- Referencing the Flink Table Store :ref:`dependencies<flink-table-store-deps>`

.. _flink-table-store-deps:

Dependencies
************

The **classpath** of kyuubi flink sql engine with Flink Table Store supported consists of

1. kyuubi-flink-sql-engine-\ |release|\ _2.12.jar, the engine jar deployed with Kyuubi distributions
2. a copy of flink distribution
3. flink-table-store-dist-<version>.jar (example: flink-table-store-dist-0.2.jar), which can be found in the `Maven Central`_

In order to make the Flink Table Store packages visible for the runtime classpath of engines, we can use these methods:

1. Put the Flink Table Store packages into ``$FLINK_HOME/lib`` directly
2. Setting the HADOOP_CLASSPATH environment variable or copy the `Pre-bundled Hadoop Jar`_ to flink/lib.

.. warning::
   Please mind the compatibility of different Flink Table Store and Flink versions, which can be confirmed on the page of `Flink Table Store multi engine support`_.

Flink Table Store Operations
------------------

Taking ``CREATE CATALOG`` as a example,

.. code-block:: sql

   CREATE CATALOG my_catalog WITH (
     'type'='table-store',
     'warehouse'='hdfs://nn:8020/warehouse/path' -- or 'file:///tmp/foo/bar'
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


.. _Flink Table Store: https://nightlies.apache.org/flink/flink-table-store-docs-stable/
.. _Official Documentation: https://nightlies.apache.org/flink/flink-table-store-docs-stable/
.. _Maven Central: https://mvnrepository.com/artifact/org.apache.flink/flink-table-store-dist
.. _Pre-bundled Hadoop Jar: https://flink.apache.org/downloads.html
.. _Flink Table Store multi engine support: https://nightlies.apache.org/flink/flink-table-store-docs-stable/docs/engines/overview/
