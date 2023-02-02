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

# SQL Lineage Support

When we execute a SQL statement, the computing engine will return to a result set,
each column in the result set may originate from different tables, and these different
tables depend on other tables, which may have been calculated by functions, aggregation, etc.
The source table is related to the result set, which is the SQL Lineage.

## Introduction

The current lineage parsing functionality is implemented as a plugin by extending Spark's `QueryExecutionListener`.
1. The `SparkListenerSQLExecutionEnd` event is triggered after the SQL execution is finished and captured by the `QueryExecuctionListener`,
where the SQL lineage parsing process is performed on the successfully executed SQL.
2. Will write the parsed lineage information to the log file in JSON format.

### Example

When the following SQL is executed:

```sql
## table
create table test_table0(a string, b string)

## query
select a as col0, b as col1 from test_table0
```

The lineage of this SQL:

```json
{
   "inputTables": ["default.test_table0"],
   "outputTables": [],
   "columnLineage": [{
      "column": "col0",
      "originalColumns": ["default.test_table0.a"]
   }, {
      "column": "col1",
      "originalColumns": ["default.test_table0.b"]
   }]
}
```

#### Lineage specific identification

- `__count__`. Means that the column is an `count(*)` aggregate expression
  and cannot extract the specific column. Lineage of the column
  like `default.test_table0.__count__`.
- `__local__`. Means that the lineage of the table is a `LocalRelation` and not the real table,
  like `__local__.a`

### SQL type support

Currently supported column lineage for spark's `Command` and `Query` type:

#### Query

- `Select`

#### Command

- `AlterViewAsCommand`
- `AppendData`
- `CreateDataSourceTableAsSelectCommand`
- `CreateHiveTableAsSelectCommand`
- `CreateTableAsSelect`
- `CreateViewCommand`
- `InsertIntoDataSourceCommand`
- `InsertIntoDataSourceDirCommand`
- `InsertIntoHadoopFsRelationCommand`
- `InsertIntoHiveDirCommand`
- `InsertIntoHiveTable`
- `MergeIntoTable`
- `OptimizedCreateHiveTableAsSelectCommand`
- `OverwriteByExpression`
- `OverwritePartitionsDynamic`
- `ReplaceTableAsSelect`
- `SaveIntoDataSourceCommand`

## Building

### Build with Apache Maven

Kyuubi Spark Lineage Listener Extension is built using [Apache Maven](https://maven.apache.org).
To build it, `cd` to the root direct of kyuubi project and run:

```shell
build/mvn clean package -pl :kyuubi-spark-lineage_2.12 -DskipTests
```

After a while, if everything goes well, you will get the plugin finally in two parts:

- The main plugin jar, which is under `./extensions/spark/kyuubi-spark-lineage/target/kyuubi-spark-lineage_${scala.binary.version}-${project.version}.jar`
- The least transitive dependencies needed, which are under `./extensions/spark/kyuubi-spark-lineage/target/scala-${scala.binary.version}/jars`

### Build against Different Apache Spark Versions

The maven option `spark.version` is used for specifying Spark version to compile with and generate corresponding transitive dependencies.
By default, it is always built with the latest `spark.version` defined in kyuubi project main pom file.
Sometimes, it may be incompatible with other Spark distributions, then you may need to build the plugin on your own targeting the Spark version you use.

For example,

```shell
build/mvn clean package -pl :kyuubi-spark-lineage_2.12 -DskipTests -Dspark.version=3.1.2
```

The available `spark.version`s are shown in the following table.

| Spark Version | Supported | Remark |
|:-------------:|:---------:|:------:|
|    master     |     √     |   -    |
|     3.3.x     |     √     |   -    |
|     3.2.x     |     √     |   -    |
|     3.1.x     |     √     |   -    |
|     3.0.x     |     √     |   -    |
|     2.4.x     |     x     |   -    |

Currently, Spark released with Scala 2.12 are supported.

### Test with ScalaTest Maven plugin

If you omit `-DskipTests` option in the command above, you will also get all unit tests run.

```shell
build/mvn clean package -pl :kyuubi-spark-lineage_2.12
```

If any bug occurs and you want to debug the plugin yourself, you can configure `-DdebugForkedProcess=true` and `-DdebuggerPort=5005`(optional).

```shell
build/mvn clean package -pl :kyuubi-spark-lineage_2.12 -DdebugForkedProcess=true
```

The tests will suspend at startup and wait for a remote debugger to attach to the configured port.

We will appreciate if you can share the bug or the fix to the Kyuubi community.

## Installing

With the `kyuubi-spark-lineage_*.jar` and its transitive dependencies available for spark runtime classpath, such as
- Copied to `$SPARK_HOME/jars`, or
- Specified to `spark.jars` configuration

## Configure

### Settings for Spark Listener Extensions

Add `org.apache.kyuubi.plugin.lineage.SparkOperationLineageQueryExecutionListener` to the spark configuration `spark.sql.queryExecutionListeners`.

```properties
spark.sql.queryExecutionListeners=org.apache.kyuubi.plugin.lineage.SparkOperationLineageQueryExecutionListener
```

### Settings for Lineage Logger and Path

#### Lineage Logger Path

The location of all the engine operation lineage events go for the builtin JSON logger.
We first need set `kyuubi.engine.event.loggers` to `JSON`.
All operation lineage events will be written in the unified event json logger path, which be setting with
`kyuubi.engine.event.json.log.path`. We can get the lineage logger from the `operation_lineage` dir in the
`kyuubi.engine.event.json.log.path`.

