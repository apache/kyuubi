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

# Introduction

This module includes TPC-DS data generator and benchmark tool.

# How to use

package jar with following command:
`./build/mvn clean package -Ptpcds -pl dev/kyuubi-tpcds -am`

## Data Generator

Support options:

|     key     |     default     |            description            |
|-------------|-----------------|-----------------------------------|
| db          | default         | the database to write data        |
| scaleFactor | 1               | the scale factor of TPC-DS        |
| format      | parquet         | the format of table to store data |
| parallel    | scaleFactor * 2 | the parallelism of Spark job      |

Example: the following command to generate 10GB data with new database `tpcds_sf10`.

```shell
$SPARK_HOME/bin/spark-submit \
  --class org.apache.kyuubi.tpcds.DataGenerator \
  kyuubi-tpcds_*.jar \
  --db tpcds_sf10 --scaleFactor 10 --format parquet --parallel 20
```

## Benchmark Tool

Support options:

|     key     |        default         |                          description                          |
|-------------|------------------------|---------------------------------------------------------------|
| db          | none(required)         | the TPC-DS database                                           |
| benchmark   | tpcds-v2.4-benchmark   | the name of application                                       |
| iterations  | 3                      | the number of iterations to run                               |
| breakdown   | false                  | whether to record breakdown results of an execution           |
| filter      | a                      | filter on the name of the queries to run, e.g. q1-v2.4        |
| results-dir | /spark/sql/performance | dir to store benchmark results, e.g. hdfs://hdfs-nn:9870/pref |

Example: the following command to benchmark TPC-DS sf10 with exists database `tpcds_sf10`.

```shell
$SPARK_HOME/bin/spark-submit \
  --class org.apache.kyuubi.tpcds.benchmark.RunBenchmark \
  kyuubi-tpcds_*.jar --db tpcds_sf10
```

We also support run one of the TPC-DS query:

```shell
$SPARK_HOME/bin/spark-submit \
  --class org.apache.kyuubi.tpcds.benchmark.RunBenchmark \
  kyuubi-tpcds_*.jar --db tpcds_sf10 --filter q1-v2.4
```

The result of TPC-DS benchmark like:

|  name   | minTimeMs | maxTimeMs  | avgTimeMs  |  stdDev  | stdDevPercent  |
|---------|-----------|------------|------------|----------|----------------|
| q1-v2.4 | 50.522384 | 868.010383 | 323.398267 | 471.6482 | 145.8413108576 |

