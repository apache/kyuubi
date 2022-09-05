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

[PySpark](https://spark.apache.org/docs/latest/api/python/index.html) PySpark is an interface for Apache Spark in Python. PySpark can connect the Kyuubi server via JDBC driver.

## Requirements
PySpark works with Python 3.7 and above. Installation of PySpark and Spark SQL is as follows:

```shell
pip install pyspark 'pyspark[sql]' 'pyspark[pandas_on_spark]'
```

## Usage
Use the Kyuubi server's host and thrift protocol port to connect.

For further information about PySpark JDBC usage and options, please refer to Spark's [JDBC To Other Databases
](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html).

### via JDBC to Kyuubi

- Put the jdbc driver jar file to `$SPARK_HOME/jars` directory to make it visible for
  the classpath of PySpark.


```python
# Loading data from a JDBC source
jdbcDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:kyuubi://kyuubi_sever_ip:kyuubi_port")
    .option("dbtable", "schema.tablename")
    .option("user", "username")
    .option("password", "password")
    .load()


# Saving data to a JDBC source
jdbcDF.write
    .format("jdbc")
    .option("url", "jdbc:kyuubi://kyuubi_sever_ip:kyuubi_port")
    .option("dbtable", "schema.tablename")
    .option("user", "username")
    .option("password", "password")
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
