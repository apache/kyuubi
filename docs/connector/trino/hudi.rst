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
and easy to expand than directly using Trino to manipulate Hudi.

Hudi Integration
----------------

To enable the integration of Kyuubi Trino SQL engine and Hudi, you need to:

- Setting the Trino extension and catalog :ref:`configurations<trino-hudi-conf>`

.. _trino-hudi-conf:

Configurations
**************

Catalogs are registered by creating a file of catalog properties in the `$TRINO_SERVER_HOME/etc/catalog` directory.
For example, we can create a `$TRINO_SERVER_HOME/etc/catalog/hudi.properties` with the following contents to mount the Hudi connector as a Hudi catalog:

.. code-block:: properties

   connector.name=hudi
   hive.metastore.uri=thrift://example.net:9083

Note: You need to replace $TRINO_SERVER_HOME above to your Trino server home path like `/opt/trino-server-406`.

More configuration properties can be found in the `Hudi connector in Trino document`_.

.. tip::
   Trino version 398 or higher, it is recommended to use the Hudi connector.
   You don't need to install any dependencies in version 398 or higher.

Hudi Operations
---------------
The globally available and read operation statements are supported in Trino.
These statements can be found in `Trino SQL Support`_.
Currently, Trino cannot write data to a Hudi table.
A common scenario is to write data with Spark/Flink and read data with Trino.
You can use the Kyuubi Trino SQL engine to query the table with the following SQL ``SELECT`` statement.

Taking ``Query Data`` as a example,

.. code-block:: sql

    USE example.example_schema;

    SELECT symbol, max(ts)
    FROM stock_ticks_cow
    GROUP BY symbol
    HAVING symbol = 'GOOG';

.. _Hudi: https://hudi.apache.org/
.. _Official Documentation: https://hudi.apache.org/docs/overview
.. _Hudi connector in Trino document: https://trino.io/docs/current/connector/hudi.html
.. _Trino SQL Support: https://trino.io/docs/current/language/sql-support.html#
