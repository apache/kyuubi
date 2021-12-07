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

# Kyuubi v.s. HiveServer2


## Introduction

HiveServer2 is a service that enables clients to execute Hive QL queries on Hive supporting multi-client concurrency and authentication.
Kyuubi enables clients to execute Spark SQL queries directly on Spark supporting multi-client concurrency and authentication too.

They are both designed to provide better support for open API clients like JDBC and ODBC to manage and analyze BigData.


## Hive on Spark

The purpose of Hive on Spark is to add Spark as a third execution backend, parallel to MR and Tez.
Comparing to Hive on MR, it's use the Spark DAG will help improve the performance of Hive queries, especially those
have multiple reducer stages.




## Differences Between Kyuubi and HiveServer2

- | Kyuubi | HiveServer2 |
--- | --- | ---
** Language ** | Spark SQL | Hive QL
** Optimizer ** | Spark SQL Catalyst | Hive Optimizer
** Engine ** | up to Spark 3.x | MapReduce/[up to Spark 2.3](https://cwiki.apache.org/confluence/display/Hive/Hive+on+Spark%3A+Getting+Started#HiveonSpark:GettingStarted-VersionCompatibility)/Tez
** Performance ** | High | Low
** Compatibility with Spark ** | Good | Bad(need to rebuild on a specific version)
** Data Types ** | [Spark Data Types](http://spark.apache.org/docs/latest/sql-ref-datatypes.html) |  [Hive Data Types](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types)


## Performance
## References

1. [HiveServer2 Overview](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Overview)
