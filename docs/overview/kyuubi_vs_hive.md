# Kyuubi v.s. HiveServer2


## Introduction

HiveServer2 is a service that enables clients to execute Hive QL queries on Hive supporting multi-client concurrency and authentication.
Kyuubi enables clients to execute Spark SQL queries directly on Spark supporting multi-client concurrency and authentication too.

They are both designed to provide better support for open API clients like JDBC and ODBC to manage and analyze BigData.


## Hive on Spark

The purpose of Hive on Spark is to add Spark as a third execution backend, parallel to MR and Tez.
Comparing to Hive on MR, it the Spark DAG will help improve the performance of Hive queries, especially those
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
