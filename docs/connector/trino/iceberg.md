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

# Iceberg

[Apache Iceberg](https://iceberg.apache.org/) is an open table format for huge analytic datasets.
Iceberg adds tables to compute engines including Spark, Trino, PrestoDB, Flink, Hive and Impala
using a high-performance table format that works just like a SQL table.

```{tip}
This article assumes that you have mastered the basic knowledge and operation of [Iceberg](https://iceberg.apache.org/).
For the knowledge about Iceberg not mentioned in this article, 
you can obtain it from its [Official Documentation](https://trino.io/docs/current/connector/iceberg.html).
```

By using kyuubi, we can run SQL queries towards Iceberg which is more
convenient, easy to understand, and easy to expand than directly using
Trino to manipulate Iceberg.

## Iceberg Integration

To enable the integration of Kyuubi Trino SQL engine and Iceberg through Catalog APIs, you need to:

- Setting the Trino extension and catalog [configurations](#configurations)

### Configurations

To activate the functionality of Iceberg, we can set the following configurations:

```properties
connector.name=iceberg
hive.metastore.uri=thrift://localhost:9083
```

## Iceberg Operations

Taking `CREATE TABLE` as an example:

```sql
CREATE TABLE orders (
  orderkey bigint,
  orderstatus varchar,
  totalprice double,
  orderdate date
) WITH (
  format = 'ORC'
);
```

Taking `SELECT` as an example:

```sql
SELECT * FROM new_orders;
```

Taking `INSERT` as an example:

```sql
INSERT INTO cities VALUES (1, 'San Francisco');
```

Taking `UPDATE` as an example:

```sql
UPDATE purchases SET status = 'OVERDUE' WHERE ship_date IS NULL;
```

Taking `DELETE FROM` as an example:

```sql
DELETE FROM lineitem WHERE shipmode = 'AIR';
```

