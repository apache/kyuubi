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

# Hive

You may know that the Apache Spark has built-in support for accessing Hive tables, it works well in most cases,
but is limited to one Hive Metastore. The Kyuubi Spark Hive connector (KSHC) implements a Hive connector based
on Spark DataSource V2 API, supports accessing multiple Hive Metastore in a single Spark application.

## Hive Integration

To enable the integration of Kyuubi Spark SQL engine and Hive connector through
Spark DataSource V2 API, you need to:

- Referencing the Hive connector [dependencies](#kshc-dependencies)
- Setting the Spark catalog [configurations](#kshc-configurations)

(kshc-dependencies)=

### Dependencies

The **classpath** of Kyuubi Spark SQL engine with Hive connector supported consists of

1. kyuubi-spark-sql-engine_2.12-{{ release }}.jar, the engine jar deployed with a Kyuubi distribution
2. a copy of Spark distribution
3. kyuubi-spark-connector-hive_2.12-{{ release }}.jar, which can be found in the [Maven Central](https://mvnrepository.com/artifact/org.apache.kyuubi/kyuubi-spark-connector-hive)

In order to make the Hive connector packages visible for the runtime classpath of engines, we can use one of these methods:

1. Put the Kyuubi Hive connector packages into `$SPARK_HOME/jars` directly
2. Set `spark.jars=/path/to/kyuubi-spark-connector-hive_2.12-<version>.jar`

```{note}
Starting from v1.9.2 and v1.10.0, KSHC jars available in the [Maven Central](https://mvnrepository.com/artifact/org.apache.kyuubi/kyuubi-spark-connector-hive) guarantee binary compatibility across
multiple Spark versions, so a single jar works across them without rebuilding.
```

(kshc-configurations)=

### Configurations

To activate functionality of Kyuubi Spark Hive connector, we can set the following configurations:

```properties
spark.sql.catalog.hive_catalog                      org.apache.kyuubi.spark.connector.hive.HiveTableCatalog
spark.sql.catalog.hive_catalog.hive.metastore.uris  thrift://metastore-host:port
spark.sql.catalog.hive_catalog.<other.hive.conf>    <value>
spark.sql.catalog.hive_catalog.<other.hadoop.conf>  <value>
```

Besides the catalog-level configurations above, the Kyuubi Spark Hive connector provides the following configurations:

|                              Key                               |      Default      |                                                                                                          Meaning                                                                                                           |  Type   | Since  |
|----------------------------------------------------------------|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|--------|
| `spark.sql.kyuubi.hive.connector.externalCatalog.share.policy` | `ONE_FOR_ALL`     | The share policy for the external catalog. `ONE_FOR_ONE` means an external catalog is used by only one `HiveTableCatalog`, while `ONE_FOR_ALL` shares an external catalog globally across the catalogs with the same name. | string  | 1.7.0  |
| `spark.sql.catalog.<catalog>.delegation.token.renewal.enabled` | `true`            | Whether to enable delegation token renewal for the Kerberized Hive Metastore of this catalog.                                                                                                                              | boolean | 1.8.0  |
| `spark.sql.kyuubi.hive.connector.read.convertMetastoreParquet` | `true`            | When enabled, the data source Parquet reader is used to process Parquet tables created by HiveQL syntax, instead of the Hive SerDe.                                                                                        | boolean | 1.11.0 |
| `spark.sql.kyuubi.hive.connector.read.convertMetastoreOrc`     | `true`            | When enabled, the data source ORC reader is used to process ORC tables created by HiveQL syntax, instead of the Hive SerDe.                                                                                                | boolean | 1.11.0 |
| `spark.sql.kyuubi.hive.connector.dropTableAsPurgeTable`        | `false`           | When enabled, `DROP TABLE` completely removes its data by skipping HDFS trash, equivalent to the `PURGE TABLE` command.                                                                                                    | boolean | 1.12.0 |
| `spark.sql.catalog.<catalog>.hive.metastore.warehouse.dir`     | &lt;undefined&gt; | The default warehouse directory for the catalog, taking precedence over the global `spark.sql.warehouse.dir` when creating a database.                                                                                     | string  | 1.12.0 |

```{note}
Catalog-level configurations (`spark.sql.catalog.<catalog>.*`) are captured when the catalog
is lazily initialized on first access, so they take effect as long as they are set before that.
Once the catalog is initialized, subsequent `SET` on these keys no longer affects it. Note that
under the default `ONE_FOR_ALL` share policy, configurations only apply to the first initialized
Hive client instance shared across catalogs with the same name.

In Spark cluster deploy mode with Kerberos and no keytab configured, all catalogs that
need HMS access must be declared with their `hive.metastore.uris` at the Spark application
bootstrap (e.g. in `spark-defaults.conf` or via `--conf`), so that Spark can fetch HMS delegation
tokens ahead of time during submission and distribute them to executors.
```

## Hive Connector Operations

Taking `CREATE NAMESPACE` as an example,

```sql
CREATE NAMESPACE ns;
```

Taking `CREATE TABLE` as an example,

```sql
CREATE TABLE hive_catalog.ns.foo (
  id bigint COMMENT 'unique id',
  data string)
USING parquet;
```

Taking `SELECT` as an example,

```sql
SELECT * FROM hive_catalog.ns.foo;
```

Taking `INSERT` as an example,

```sql
INSERT INTO hive_catalog.ns.foo VALUES (1, 'a'), (2, 'b'), (3, 'c');
```

Taking `DROP TABLE` as an example,

```sql
DROP TABLE hive_catalog.ns.foo;
```

Taking `DROP NAMESPACE` as an example,

```sql
DROP NAMESPACE hive_catalog.ns;
```

## Features

Since v1.11.0, KSHC uses the data source Parquet/ORC reader to process Parquet and ORC tables
created by HiveQL syntax by default, instead of the Hive SerDe. This enables vectorized reading.
Set `spark.sql.kyuubi.hive.connector.read.convertMetastoreParquet` and
`spark.sql.kyuubi.hive.connector.read.convertMetastoreOrc` to `false` to fall back to the Hive
SerDe.

Since v1.12.0, KSHC supports the `PURGE TABLE` command. By default, `DROP TABLE` moves the table
data to the HDFS trash. Set `spark.sql.kyuubi.hive.connector.dropTableAsPurgeTable` to `true` to
make `DROP TABLE` skip the HDFS trash and completely remove its data, behaving like `PURGE TABLE`.

Since v1.13.0, KSHC supports Dynamic Partition Pruning (DPP) for partitioned Hive tables, which
significantly reduces the amount of data scanned when joining against large partitioned tables.

## Advanced Usages

Though KSHC is a pure Spark DataSource V2 connector which isn't coupled with Kyuubi deployment, due to the
implementation inside `spark-sql`, you should not expect KSHC works properly with `spark-sql`, and
any issues caused by such a combination usage won't be considered at this time. Instead, it's recommended
using BeeLine with Kyuubi as a drop-in replacement for `spark-sql`, or switching to `spark-shell`.

KSHC supports accessing Kerberized Hive Metastore and HDFS, by using keytab, or TGT cache, or Delegation Token.
It's not expected to work properly with multiple KDC instances, the limitation comes from JDK Krb5LoginModule,
for such cases, consider setting up Cross-Realm Kerberos trusts, then you just need to talk with one KDC.

For HMS Thrift API used by Spark, it's known that Hive 2.3.9 client is compatible with HMS from 2.1 to 3.1, and
Hive 2.3.10 client is compatible with HMS from 1.1 to 3.1, such version combinations should cover the most cases.
For other corner cases, KSHC also supports `spark.sql.catalog.<catalog_name>.spark.sql.hive.metastore.jars` and
`spark.sql.catalog.<catalog_name>.spark.sql.hive.metastore.version` as well as the Spark built-in Hive datasource
does, you can refer to the Spark documentation for details.

## Limitations

Currently, KSHC has the following limitations:

- Persistent Hive views and UDFs are not supported through a KSHC catalog, so `CREATE VIEW`,
  `SHOW VIEWS`, `CREATE FUNCTION`, and `SHOW FUNCTIONS` all fail when the target or current
  catalog is a KSHC catalog. Temporary views and temporary functions are not affected. As a
  workaround, persistent views and UDFs can be created and accessed through the Spark built-in
  `spark_catalog`.
- Bucket tables are not supported and are handled as regular Hive tables.

