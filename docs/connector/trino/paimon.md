<!--
- Licensed to the Apache Software Foundation (ASF) under one or more
- contributor license agreements.  See the NOTICE file distributed with
- this work for additional information regarding copyright ownership.
- The ASF licenses this file to You under the Apache License, Version 2.0
- (the "License"); you may not use this file except in compliance with
- the License.  You may obtain a copy of the License at
-
-   http://www.apache.org/licenses/LICENSE-2.0
-
- Unless required by applicable law or agreed to in writing, software
- distributed under the License is distributed on an "AS IS" BASIS,
- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
- See the License for the specific language governing permissions and
- limitations under the License.
-->

# Apache Paimon

[Apache Paimon](https://paimon.apache.org/) is a streaming data lake platform that supports high-speed data ingestion, change data tracking and efficient real-time analytics.

```{tip}
This article assumes that you have mastered the basic knowledge and operation of [Apache Paimon](https://paimon.apache.org/).
For the knowledge about Apache Paimon not mentioned in this article, 
you can obtain it from its [Official Documentation](https://paimon.apache.org/docs/master/).
```

By using kyuubi, we can run SQL queries towards Apache Paimon which is more
convenient, easy to understand, and easy to expand than directly using
Trino to manipulate Apache Paimon.

## Apache Paimon Integration

To enable the integration of Kyuubi Trino SQL engine and Apache Paimon, you need to:

- Referencing the Apache Paimon [dependencies](#dependencies)
- Setting the Trino extension and catalog [configurations](#configurations)

### Dependencies

The **classpath** of Kyuubi Trino SQL engine with Apache Paimon supported consists of:

1. kyuubi-trino-sql-engine-{{ release }}_2.12.jar, the engine jar deployed with a Kyuubi distribution
2. A copy of Trino distribution
3. `paimon-trino-<version>.jar` (example: `paimon-trino-0.2.jar`), which code can be found in the [Source Code](https://github.com/apache/paimon-trino)
4. `flink-shaded-hadoop-2-uber-<version>.jar`, which code can be found in the [Pre-bundled Hadoop](https://flink.apache.org/downloads/#additional-components)

In order to make the Apache Paimon packages visible for the runtime classpath of engines, you need to:

1. Build the `paimon-trino-<version>.jar` by reference to [Apache Paimon Trino README](https://github.com/apache/paimon-trino)
2. Put the `paimon-trino-<version>.jar` and `flink-shaded-hadoop-2-uber-<version>.jar` packages into `$TRINO_SERVER_HOME/plugin/tablestore` directly

```{warning}
Please mind the compatibility of different Apache Paimon and Trino versions,
which can be confirmed on the page of [Apache Paimon multi engine support](https://paimon.apache.org/docs/master/engines/overview/).
```

### Configurations

To activate the functionality of Apache Paimon, we can set the following configurations:

Catalogs are registered by creating a catalog properties file in the `$TRINO_SERVER_HOME/etc/catalog` directory.
For example, create `$TRINO_SERVER_HOME/etc/catalog/tablestore.properties` with the following contents to mount the tablestore connector as the tablestore catalog:

```properties
connector.name=tablestore
warehouse=file:///tmp/warehouse
```

## Apache Paimon Operations

Apache Paimon supports reading table store tables through Trino.
A common scenario is to write data with Spark or Flink and read data with Trino.
You can follow this document [Apache Paimon Engines Flink Quick Start](https://paimon.apache.org/docs/master/engines/flink/#quick-start) to
write data to a table store table and then use Kyuubi Trino SQL engine to query
the table with the following SQL `SELECT` statement.

```sql
SELECT * FROM tablestore.default.t1
```

