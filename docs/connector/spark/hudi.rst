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

`Hudi`_
========

Apache Hudi (pronounced “hoodie”) is the next generation streaming data lake platform.
Apache Hudi brings core warehouse and database functionality directly to a data lake.

.. tip::
   This article assumes that you have mastered the basic knowledge and operation of `Hudi`_.
   For the knowledge about Hudi not mentioned in this article,
   you can obtain it from its `Official Documentation`_.

By using Kyuubi, we can run SQL queries towards Hudi which is more convenient, easy to understand,
and easy to expand than directly using Spark to manipulate Hudi.

Hudi Integration
----------------

To enable the integration of Kyuubi Spark SQL engine and Hudi through
Spark DataSource V2 API, you need to:

- Referencing the Hudi :ref:`dependencies<spark-hudi-deps>`
- Setting the Spark extension and catalog :ref:`configurations<spark-hudi-conf>`

.. _spark-hudi-deps:

Dependencies
************

The **classpath** of Kyuubi Spark SQL engine with Hudi supported consists of

1. kyuubi-spark-sql-engine-\ |release|\ _2.12.jar, the engine jar deployed with a Kyuubi distribution
2. a copy of Spark distribution
3. hudi-spark<spark.version>-bundle_<scala.version>-<hudi.version>.jar (example: hudi-spark3.5-bundle_2.12:1.0.1.jar), which can be found in the `Maven Central`_

In order to make the Hudi packages visible for the runtime classpath of engines, we can use one of these methods:

1. Put the Hudi packages into ``$SPARK_HOME/jars`` directly
2. Set ``spark.jars=/path/to/hudi-spark-bundle``

.. _spark-hudi-conf:

Configurations
**************

To activate functionality of Hudi, we can set the following configurations:

.. code-block:: properties

   # Spark 3.2 to 3.5
   spark.serializer=org.apache.spark.serializer.KryoSerializer
   spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar
   spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
   spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog

Hudi Operations
---------------

Taking ``Create Table`` as a example,

.. code-block:: sql

   CREATE TABLE hudi_cow_nonpcf_tbl (
     uuid INT,
     name STRING,
     price DOUBLE
   ) USING HUDI;

Taking ``Query Data`` as a example,

.. code-block:: sql

   SELECT * FROM hudi_cow_nonpcf_tbl WHERE id < 20;

Taking ``Insert Data`` as a example,

.. code-block:: sql

   INSERT INTO hudi_cow_nonpcf_tbl SELECT 1, 'a1', 20;


Taking ``Update Data`` as a example,

.. code-block:: sql

   UPDATE hudi_cow_nonpcf_tbl SET name = 'foo', price = price * 2 WHERE id = 1;

Taking ``Delete Data`` as a example,

.. code-block:: sql

   DELETE FROM hudi_cow_nonpcf_tbl WHERE uuid = 1;

.. _Hudi: https://hudi.apache.org/
.. _Official Documentation: https://hudi.apache.org/docs/overview
.. _Maven Central: https://mvnrepository.com/artifact/org.apache.hudi
