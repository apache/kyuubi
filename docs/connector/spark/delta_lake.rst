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

`Delta Lake`_
=============

Delta lake is an open-source project that enables building a Lakehouse
Architecture on top of existing storage systems such as S3, ADLS, GCS,
and HDFS.

.. tip::
   This article assumes that you have mastered the basic knowledge and
   operation of `Delta Lake`_.
   For the knowledge about delta lake not mentioned in this article,
   you can obtain it from its `Official Documentation`_.

By using kyuubi, we can run SQL queries towards delta lake which is more
convenient, easy to understand, and easy to expand than directly using
spark to manipulate delta lake.

Delta Lake Integration
----------------------

To enable the integration of kyuubi spark sql engine and delta lake through
Apache Spark Datasource V2 and Catalog APIs, you need to:

- Referencing the delta lake :ref:`dependencies<spark-delta-lake-deps>`
- Setting the spark extension and catalog :ref:`configurations<spark-delta-lake-conf>`

.. _spark-delta-lake-deps:

Dependencies
************

The **classpath** of kyuubi spark sql engine with delta lake supported consists of

1. kyuubi-spark-sql-engine-\ |release|\ _2.12.jar, the engine jar deployed with kyuubi distributions
2. a copy of spark distribution
3. delta-core & delta-storage, which can be found in the `Maven Central`_

In order to make the delta packages visible for the runtime classpath of engines, we can use one of these methods:

1. Put the delta packages into ``$SPARK_HOME/jars`` directly
2. Set ``spark.jars=/path/to/delta-core,/path/to/delta-storage``

.. warning::
   Please mind the compatibility of different Delta Lake and Spark versions, which can be confirmed on the page of `delta release notes`_.

.. _spark-delta-lake-conf:

Configurations
**************

To activate functionality of delta lake, we can set the following configurations:

.. code-block:: properties

   spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
   spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

Delta Lake Operations
---------------------

As for end-users, who only use a pure SQL interface, there aren't much differences between
using a delta table and a regular hive table. Unless you are going to use some advanced
features, but they are still SQL, just more syntax added.

Taking ``CREATE A TABLE`` as a example,

.. code-block:: sql

   CREATE TABLE IF NOT EXISTS kyuubi_delta (
     id INT,
     name STRING,
     org STRING,
     url STRING,
     start TIMESTAMP
   ) USING DELTA;

.. _Delta Lake: https://delta.io/
.. _Official Documentation: https://docs.delta.io/latest/index.html
.. _Maven Central: https://mvnrepository.com/artifact/io.delta/delta-core
.. _Delta release notes: https://github.com/delta-io/delta/releases