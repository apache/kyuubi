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
you can obtain it from its [Official Documentation](https://iceberg.apache.org/docs/latest/).
```

By using kyuubi, we can run SQL queries towards Iceberg which is more
convenient, easy to understand, and easy to expand than directly using
hive to manipulate Iceberg.

## Iceberg Integration

To enable the integration of Kyuubi Hive SQL engine and Iceberg, you need to:

- Referencing the Iceberg [dependencies](#dependencies)
- Setting the Hive extension and catalog [configurations](#configurations)

### Dependencies

The **classpath** of Kyuubi Hive SQL engine with Iceberg supported consists of:

1. kyuubi-hive-sql-engine-{{ release }}_2.12.jar, the engine jar deployed with a Kyuubi distribution
2. A copy of Hive distribution
3. `iceberg-hive-runtime-<hive.version>_<scala.version>-<iceberg.version>.jar` (example: `iceberg-hive-runtime-3.2_2.12-0.14.0.jar`), which can be found in the [Maven Central](https://mvnrepository.com/artifact/org.apache.iceberg)

In order to make the Iceberg packages visible for the runtime classpath of engines, we can use the method:

1. Execute `ADD JAR` statement in the Kyuubi to add dependencies to Hive's auxiliary classpath. For example:

   ```sql
   ADD JAR /path/to/iceberg-hive-runtime.jar;
   ```

```{warning}
Please mind the compatibility of different Iceberg and Hive versions, which can
be confirmed on the page of [Iceberg multi engine support](https://iceberg.apache.org/multi-engine-support/).
```

### Configurations

To activate functionality of Iceberg, we can set the following configurations:
Set `iceberg.engine.hive.enabled=true` in the Hadoop configuration. For example, setting this in the `hive-site.xml`.

## Iceberg Operations

Taking `CREATE TABLE` as an example:

```sql
CREATE TABLE x (i int) STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler';
CREATE EXTERNAL TABLE x (i int) STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler';
```

Taking `SELECT` as an example:

```sql
SELECT * FROM foo;
```

Taking `INSERT` as an example:

```sql
INSERT INTO foo VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT OVERWRITE TABLE target SELECT * FROM source;
```

Taking `ALTER` as an example:

```sql
ALTER TABLE t SET TBLPROPERTIES('...'='...');
```

