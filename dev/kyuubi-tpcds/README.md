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
This module includes tpcds data generator and benchmark.

# How to use

package jar with following command:
`./build/mvn install -DskipTests -Ptpcds -pl dev/kyuubi-tpcds -am`

## data generator 
Run following command to generate 10GB data with new database `tpcds_sf10`.

```shell
$SPARK_HOME/bin/spark-submit \
  --conf spark.sql.tpcds.scale.factor=10 \
  --conf spark.sql.tpcds.database=tpcds_sf10 \
  --class org.apache.kyuubi.tpcds.DataGenerator \
  kyuubi-tpcds-*.jar
```

## do benchmark

Run following command to benchmark tpcds sf10 with exists database `tpcds_sf10`.

```shell
$SPARK_HOME/bin/spark-submit \
  --class org.apache.kyuubi.tpcds.benchmark.RunBenchmark \
  kyuubi-tpcds-*.jar --db tpcds_sf10
```

We also support run one of the tpcds query:
```shell
$SPARK_HOME/bin/spark-submit \
  --class org.apache.kyuubi.tpcds.benchmark.RunBenchmark \
  kyuubi-tpcds-*.jar --db tpcds_sf10 --filter q1-v2.4
```

The result of tpcds benchmark like:

| name    | minTimeMs |  maxTimeMs  |  avgTimeMs | stdDev   | stdDevPercent  |
|---------|-----------|-------------|------------|----------|----------------|
| q1-v2.4 | 50.522384 |  868.010383 | 323.398267 | 471.6482 | 145.8413108576 |
