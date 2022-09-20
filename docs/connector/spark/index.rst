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

Connectors for Spark SQL Query Engine
=====================================

The Kyuubi Spark SQL Query Engine uses Spark DataSource APIs(V1/V2) to access
data from different data sources.

By default, it provides accessibility to hive warehouses with various file formats
supported, such as parquet, orc, json, etc.

Also，it can easily integrate with other third-party libraries, such as Hudi,
Iceberg, Delta Lake, Kudu, Flink Table Store, HBase，Cassandra, etc.

We also provide sample data sources like TDC-DS, TPC-H for testing and benchmarking
purpose.

.. toctree::
    :maxdepth: 2

    delta_lake
    delta_lake_with_azure_blob
    hudi
    iceberg
    kudu
    hive
    flink_table_store
    tidb
    tpcds
    tpch
