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

You may know that the Apache Spark has built-in support for accessing Hive tables, it works well in most cases,
but is limited to one Hive Metastore. The Kyuubi Spark Hive connector(KSHC) implemented a Hive connector based
on Spark DataSource V2 API, supports accessing multiple Hive Metastore in a single Spark application.

Hive Integration
----------------

To enable the integration of Kyuubi Spark SQL engine and Hive connector through
Spark DataSource V2 API, you need to:

- Referencing the Hive connector :ref:`dependencies<kyuubi-hive-deps>`
- Setting the Spark catalog :ref:`configurations<kyuubi-hive-conf>`

.. _kyuubi-hive-deps:

Dependencies
************

The **classpath** of Kyuubi Spark SQL engine with Hive connector supported consists of

1. kyuubi-spark-sql-engine-\ |release|\ _2.12.jar, the engine jar deployed with a Kyuubi distribution
2. a copy of Spark distribution
3. kyuubi-spark-connector-hive_2.12-\ |release|\ , which can be found in the `Maven Central`_

In order to make the Hive connector packages visible for the runtime classpath of engines, we can use one of these methods:

1. Put the Kyuubi Hive connector packages into ``$SPARK_HOME/jars`` directly
2. Set ``spark.jars=/path/to/kyuubi-hive-connector``

.. note::
   Starting from v1.9.2 and v1.10.0, KSHC jars available in the `Maven Central`_ guarantee binary compatibility across
   Spark versions, namely, Spark 3.3 onwards.

.. _kyuubi-hive-conf:

Configurations
**************

To activate functionality of Kyuubi Spark Hive connector, we can set the following configurations:

.. code-block:: properties

   spark.sql.catalog.hive_catalog                      org.apache.kyuubi.spark.connector.hive.HiveTableCatalog
   spark.sql.catalog.hive_catalog.hive.metastore.uris  thrift://metastore-host:port
   spark.sql.catalog.hive_catalog.<other.hive.conf>    <value>
   spark.sql.catalog.hive_catalog.<other.hadoop.conf>  <value>

Hive Connector Operations
-------------------------

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

Advanced Usages
***************

Though KSHC is a pure Spark DataSource V2 connector which isn't coupled with Kyuubi deployment, due to the
implementation inside ``spark-sql``, you should not expect KSHC works properly with ``spark-sql``, and
any issues caused by such a combination usage won't be considered at this time. Instead, it's recommended
using BeeLine with Kyuubi as a drop-in replacement for ``spark-sql``, or switching to ``spark-shell``.

KSHC supports accessing Kerberized Hive Metastore and HDFS, by using keytab, or TGT cache, or Delegation Token.
It's not expected to work properly with multiple KDC instances, the limitation comes from JDK Krb5LoginModule,
for such cases, consider setting up Cross-Realm Kerberos trusts, then you just need to talk with one KDC.

For HMS Thrift API used by Spark, it's known that Hive 2.3.9 client is compatible with HMS from 2.1 to 4.0, and
Hive 2.3.10 client is compatible with HMS from 1.1 to 4.0, such version combinations should cover the most cases.
For other corner cases, KSHC also supports ``spark.sql.catalog.<catalog_name>.spark.sql.hive.metastore.jars`` and
``spark.sql.catalog.<catalog_name>.spark.sql.hive.metastore.version`` as well as the Spark built-in Hive datasource
does, you can refer to the Spark documentation for details.

Currently, KSHC has not implemented the Parquet/ORC Hive tables read/write optimization, in other words, it always
uses Hive SerDe to access Hive tables, so there might be a performance gap compared to the Spark built-in Hive
datasource, especially due to lack of support for vectorized reading. And you may hit bugs caused by Hive SerDe,
e.g. ``ParquetHiveSerDe`` can not read Parquet files that decimals are written in int-based format produced by
Spark Parquet datasource writer with ``spark.sql.parquet.writeLegacyFormat=false``.

.. _Apache Spark: https://spark.apache.org/
.. _Maven Central: https://mvnrepository.com/artifact/org.apache.kyuubi/kyuubi-spark-connector-hive
