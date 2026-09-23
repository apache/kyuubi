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
spark to manipulate Apache Paimon.

## Apache Paimon Integration

To enable the integration of Kyuubi Spark SQL engine and Apache Paimon through Spark DataSource V2 API, you need to:

- Referencing the Apache Paimon [dependencies](#dependencies)
- Setting the Spark extension and catalog [configurations](#configurations)

### Dependencies

The **classpath** of Kyuubi Spark SQL engine with Apache Paimon consists of:

1. kyuubi-spark-sql-engine-{{ release }}_2.12.jar, the engine jar deployed with a Kyuubi distribution
2. A copy of Spark distribution
3. `paimon-spark-<version>.jar` (example: `paimon-spark-3.5-0.8.1.jar`), which can be found in the [Apache Paimon Supported Engines Spark3](https://paimon.apache.org/docs/master/engines/spark3/)

In order to make the Apache Paimon packages visible for the runtime classpath of engines, we can use one of these methods:

1. Put the Apache Paimon packages into `$SPARK_HOME/jars` directly
2. Set `spark.jars=/path/to/paimon-spark-<version>.jar`

```{warning}
Please mind the compatibility of different Apache Paimon and Spark versions,
which can be confirmed on the page of [Apache Paimon multi engine support](https://paimon.apache.org/docs/master/engines/overview/).
```

### Configurations

To activate functionality of Apache Paimon, we can set the following configurations:

```properties
spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog
spark.sql.catalog.paimon.warehouse=file:/tmp/paimon
```

## Apache Paimon Operations

Taking `CREATE NAMESPACE` as an example:

```sql
CREATE DATABASE paimon.default;
USE paimon.default;
```

Taking `CREATE TABLE` as an example:

```sql
create table my_table (
    k int,
    v string
) tblproperties (
    'primary-key' = 'k'
);
```

Taking `SELECT` as an example:

```sql
SELECT * FROM my_table;
```

Taking `INSERT` as an example:

```sql
INSERT INTO my_table VALUES (1, 'Hi Again'), (3, 'Test');
```

