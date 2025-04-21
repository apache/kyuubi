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

# Hive JDBC Data Source Dialect

Hive JDBC Data Source dialect plugin aims to provide Hive Dialect support to [Spark's JDBC Data Source](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html).
It will auto registered to Spark and applied to JDBC sources with url prefix of `jdbc:hive2://` or `jdbc:kyuubi://`.

Hive Dialect helps to solve failures access Kyuubi. It fails and unexpected results when querying data from Kyuubi as
JDBC data source with Hive JDBC Driver or Kyuubi Hive JDBC Driver in Spark, as Spark JDBC provides no Hive Dialect support
out of box and quoting columns and other identifiers in ANSI as "table.column" rather than in HiveSQL style as
\`table\`.\`column\`.

Notes: this is an inefficient way to access data stored in Hive warehouse, you can see more discussions at [SPARK-47482](https://github.com/apache/spark/pull/45609).

## Features

- Quote identifier in Hive SQL style

  e.g. Quote `table.column` in \`table\`.\`column\`

- Adapt to Hive data type definitions

  Reference: https://cwiki.apache.org/confluence/display/hive/languagemanual+types

## Preparation

### Prepare JDBC driver

Prepare JDBC driver jar file. Supported Hive compatible JDBC Driver as below:

| Driver                                                    |            Driver Class Name            | Remarks                                                                                                  |
|-----------------------------------------------------------|-----------------------------------------|----------------------------------------------------------------------------------------------------------|
| Kyuubi Hive JDBC Driver ([doc](../jdbc/kyuubi_jdbc.html)) | org.apache.kyuubi.jdbc.KyuubiHiveDriver | Use v1.6.1 or later versions, which includes [KYUUBI #3484](https://github.com/apache/kyuubi/pull/3485). |
| Hive JDBC Driver ([doc](../jdbc/hive_jdbc.html))          | org.apache.hive.jdbc.HiveDriver         | The Hive JDBC driver is already included in official Spark binary distribution.                          |

Refer to docs of the driver and prepare the JDBC driver jar file.

### Prepare JDBC Hive Dialect extension

Prepare the plugin jar file `kyuubi-extension-spark-jdbc-dialect_-*.jar`.

Get the Kyuubi Hive Dialect Extension jar from Maven Central

```
<dependency>
    <groupId>org.apache.kyuubi</groupId>
    <artifactId>kyuubi-extension-spark-jdbc-dialect_2.12</artifactId>
    <version>{latest-version}</version>
</dependency>
```

Or, compile the extension by executing

```
build/mvn clean package -pl :kyuubi-extension-spark-jdbc-dialect_2.12 -DskipTests
```
then get the extension jar under `extensions/spark/kyuubi-extension-spark-jdbc-dialect/target`.

If you like, you can compile the extension jar with the corresponding Maven's profile on you compile command,
i.e. you can get extension jar for Spark 3.5 by compiling with `-Pspark-3.5`

### Including jars of JDBC driver and Hive Dialect extension

Choose one of the following ways to include jar files in Spark.

- Put the jar file of JDBC driver and Hive Dialect to `$SPARK_HOME/jars` directory to make it visible for all Spark applications. And adding `spark.sql.extensions = org.apache.spark.sql.dialect.KyuubiSparkJdbcDialectExtension` to `$SPARK_HOME/conf/spark_defaults.conf.`

- With each `spark-submit`(or `spark-sql`, `pyspark` etc.) commands, include the JDBC driver when submitting the application with `--packages`, and the Hive Dialect plugins with `--jars`

```
$SPARK_HOME/bin/spark-submit \
  --packages org.apache.hive:hive-jdbc:x.y.z \
  --jars /path/kyuubi-extension-spark-jdbc-dialect_-*.jar \
  ...
```

- Setting jars and config with SparkSession builder

```
val spark = SparkSession.builder
    .config("spark.jars", "/path/hive-jdbc-x.y.z.jar,/path/kyuubi-extension-spark-jdbc-dialect_-*.jar")
    .config("spark.sql.extensions", "org.apache.spark.sql.dialect.KyuubiSparkJdbcDialectExtension")
    .getOrCreate()
```

## Usage

### Using as JDBC Datasource programmingly

```
# Loading data from Kyuubi via HiveDriver as JDBC datasource
val jdbcDF = spark.read
    .format("jdbc")
    .option("driver", "org.apache.hive.jdbc.HiveDriver")
    .option("url", "jdbc:hive2://kyuubi_server_ip:port")
    .option("dbtable", "schema.tablename")
    .option("user", "username")
    .option("password", "password")
    .option("query", "select * from testdb.src_table")
    .load()
```

### Using as JDBC Datasource table with SQL

From Spark 3.2.0, [`CREATE DATASOURCE TABLE`](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html)
is supported to create jdbc source with SQL.

```sql
-- create JDBC data source table
CREATE TABLE kyuubi_table
USING JDBC
OPTIONS (
    driver='org.apache.hive.jdbc.HiveDriver',
    url='jdbc:hive2://kyuubi_server_ip:port',
    user='user',
    password='password',
    dbtable='testdb.some_table'
)

-- query data
SELECT * FROM kyuubi_table

-- write data in overwrite mode
INSERT OVERWRITE kyuubi_table SELECT ...

-- write data in append mode
INSERT INTO kyuubi_table SELECT ...
```
