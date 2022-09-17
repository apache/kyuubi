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


# PySpark

[PySpark](https://spark.apache.org/docs/latest/api/python/index.html) is an interface for Apache Spark in Python. PySpark can connect the Kyuubi server via JDBC driver.

## Requirements
PySpark works with Python 3.7 and above.

Install PySpark with Spark SQL and optional pandas on Spark using PyPI as follows:

```shell
pip install pyspark 'pyspark[sql]' 'pyspark[pandas_on_spark]'
```

For installation using Conda or manually downloading, please refer to [PySpark installation](https://spark.apache.org/docs/latest/api/python/getting_started/install.html).

## Preperation


### Prepare JDBC driver 
Prepare JDBC driver jar file. Supported Hive compatible JDBC Driver as below:

| Driver     | Driver Class Name | Remarks|
| ---------- | ----------------- | ----- |
| Kyuubi Hive Driver ([doc](../jdbc/kyuubi_jdbc.html))| org.apache.kyuubi.jdbc.KyuubiHiveDriver | 
| Hive Driver ([doc](../jdbc/hive_jdbc.html))| org.apache.hive.jdbc.HiveDriver | Compile for the driver on master branch, as [KYUUBI #3484](https://github.com/apache/incubator-kyuubi/pull/3485) required by Spark JDBC source not yet inclueded in release version.

Refer docs of the driver and prepare the JDBC driver jar file.


### Prepare Hive Dialect

Hive Dialect support is requried by Spark for wraping sql correctly and sending to JDBC driver. Kyuubi provides a JDBC dialect plugin with auto regiesting Hive Daliect support for Spark. Follow the instrunctions in [Hive Dialect Support](../../engines/spark/jdbc-dialect.html) to prepare the plugin jar file `kyuubi-extension-spark-jdbc-dialect_-*.jar`.

### Including jars of JDBC driver and Hive Dialect

Choose one of following ways to regeister driver to Spark,

- Put the jar file of JDBC driver and Hive Dialect to `$SPARK_HOME/jars` directory to make it visible for the classpath of PySpark. And adding `spark.sql.extensions = org.apache.spark.sql.dialect.KyuubiSparkJdbcDialectExtension` to `$SPARK_HOME/conf/spark_defaults.conf.`

- With spark's start shell, include JDBC driver when you submit the application with `--packages`, and the Hive Dialect plugins with `--jars` and `--driver-class-path`

```
 $SPARK_HOME/bin/pyspark --packages org.apache.hive:hive-jdbc:x.y.z --driver-class-path /path/kyuubi-extension-spark-jdbc-dialect_-*.jar --jars /path/kyuubi-extension-spark-jdbc-dialect_-*.jar
```

- Setting jars and config with SparkSession builder

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .config("spark.jars", "/path/hive-jdbc-x.y.z.jar,/path/hive-service-x.y.z.jar", "/path/kyuubi-extension-spark-jdbc-dialect_-*.jar") \
        .config("spark.driver.extraClassPath", "/path/hive-jdbc-x.y.z.jar,/path/hive-service-x.y.z.jar", "/path/kyuubi-extension-spark-jdbc-dialect_-*.jar") \
        .config("spark.sql.extensions", "org.apache.spark.sql.dialect.KyuubiSparkJdbcDialectExtension") \
        .getOrCreate()
```



## Usage

For further information about PySpark JDBC usage and options, please refer to Spark's [JDBC To Other Databases](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html).

### Reading and Writing dataframe via JDBC data source

```python
# Loading data from Kyuubi via HiveDriver as JDBC source
jdbcDF = spark.read \
  .format("jdbc") \
  .options(driver="org.apache.hive.jdbc.HiveDriver",
           url="jdbc:hive2://kyuubi_server_ip:port",
           user="user",
           password="password",
           query="select * from testdb.src_table"
           ) \
  .load()


# Saving data to Kyuubi via HiveDriver as JDBC source
jdbcDF.write \
    .format("jdbc") \
    .options(driver="org.apache.hive.jdbc.HiveDriver",
             url="jdbc:hive2://kyuubi_server_ip:port",
           user="user",
           password="password",
           dbtable="testdb.tgt_table"
           ) \
    .save()
```


### Use PySpark with Pandas
From PySpark 3.2.0, PySpark supports pandas API on Spark which allows you to scale your pandas workload out.

Pandas-on-Spark DataFrame and Spark DataFrame are virtually interchangeable.More instructions in [From/to pandas and PySpark DataFrames](https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/pandas_pyspark.html#pyspark).


```python
import pyspark.pandas as ps

psdf = ps.range(10)
sdf = psdf.to_spark().filter("id > 5")
sdf.show()
```
