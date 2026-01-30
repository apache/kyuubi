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

| key         | default                | description                                                                                                         |
|-------------|------------------------|---------------------------------------------------------------------------------------------------------------------|
| db          | none(required)         | the TPC-DS database                                                                                                 |
| benchmark   | tpcds-v2.4-benchmark   | the name of application                                                                                             |
| iterations  | 3                      | the number of iterations to run                                                                                     |
| breakdown   | false                  | whether to record breakdown results of an execution                                                                 |
| results-dir | /spark/sql/performance | dir to store benchmark results, e.g. hdfs://hdfs-nn:9870/pref                                                       |
| include     | none(optional)         | name of the queries to run, use comma to split multiple names, e.g. q1,q2                                           |
| exclude     | none(optional)         | name of the queries to exclude, use comma to split multiple names, e.g. q2,q4                                       |
| execution-mode     | collect         | how a given Spark benchmark should be run, only the following four modes are supported: collect,foreach,saveToParquet,hash |

Example: the following command to benchmark TPC-DS sf10 with exists database `tpcds_sf10`.

```shell
$SPARK_HOME/bin/spark-submit \
  --class org.apache.kyuubi.tpcds.benchmark.RunBenchmark \
  kyuubi-tpcds_*.jar --db tpcds_sf10
```

We also support run specified SQL collections of the TPC-DS query:

```shell
$SPARK_HOME/bin/spark-submit \
  --class org.apache.kyuubi.tpcds.benchmark.RunBenchmark \
  kyuubi-tpcds_*.jar --db tpcds_sf10 --include q1,q2
```

The result of TPC-DS benchmark like:

| name    | minTimeMs    | maxTimeMs    | avgTimeMs        | stdDev           | stdDevPercent    |
|---------|--------------|--------------|------------------|------------------|------------------|
| q1-v2.4 | 8329.884508  | 14159.307004 | 10537.235825     | 3161.74253777417 | 30.0054263782615 |
| q2-v2.4 | 16600.979609 | 18932.613523 | 18137.6516166666 | 1331.06332796139 | 7.33867512781137 |

If you want to exclude some SQL, you can use exclude:

```shell
$SPARK_HOME/bin/spark-submit \
  --class org.apache.kyuubi.tpcds.benchmark.RunBenchmark \
  kyuubi-tpcds_*.jar --db tpcds_sf10 --exclude q2,q4
```

The result of TPC-DS benchmark like:

| name     | minTimeMs    | maxTimeMs    | avgTimeMs        | stdDev           | stdDevPercent     |
|----------|--------------|--------------|------------------|------------------|-------------------|
| q1-v2.4  | 8329.884508  | 14159.307004 | 10537.235825     | 3161.74253777417 | 30.0054263782615  |
| q3-v2.4  | 3841.009061  | 4685.16345   | 4128.583224      | 482.102016761038 | 11.6771781166603  |
| q5-v2.4  | 39405.654981 | 48845.359253 | 43530.6847113333 | 4830.98802198401 | 11.0978911864583  |
| q6-v2.4  | 2998.962221  | 7793.096796  | 4658.37355366666 | 2716.310089792   | 58.3102677039276  |
| ...      | ...          | ...          | ...              | ...              | ...               |
| q99-v2.4 | 11747.22389  | 11900.570288 | 11813.018609     | 78.9544389266673 | 0.668368022941351 |

When both include and exclude exist simultaneously, the final SQL collections executed is include minus exclude:

```shell
$SPARK_HOME/bin/spark-submit \
  --class org.apache.kyuubi.tpcds.benchmark.RunBenchmark \
  kyuubi-tpcds_*.jar --db tpcds_sf10 --include q1,q2,q3,q4,q5 --exclude q2,q4
```

The result of TPC-DS benchmark like:

| name     | minTimeMs    | maxTimeMs    | avgTimeMs        | stdDev           | stdDevPercent     |
|----------|--------------|--------------|------------------|------------------|-------------------|
| q1-v2.4  | 8329.884508  | 14159.307004 | 10537.235825     | 3161.74253777417 | 30.0054263782615  |
| q3-v2.4  | 3841.009061  | 4685.16345   | 4128.583224      | 482.102016761038 | 11.6771781166603  |
| q5-v2.4  | 39405.654981 | 48845.359253 | 43530.6847113333 | 4830.98802198401 | 11.0978911864583  |