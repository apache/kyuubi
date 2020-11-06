# Kyuubi v.s. HiveServer2

## Differences Between Kyuubi and HiveServer2

- | Kyuubi | HiveServer2 |
--- | --- | ---
** Language ** | Spark SQL | Hive QL
** Optimizer ** | Spark SQL Catalyst | Hive Optimizer
** Engine ** | up to Spark 3.x | MapReduce/[up to Spark 2.3](https://cwiki.apache.org/confluence/display/Hive/Hive+on+Spark%3A+Getting+Started#HiveonSpark:GettingStarted-VersionCompatibility)/Tez
** Performance ** | High | Low
** Compatibility w/ Spark ** | Good | Bad(need to rebuild on a specific version)
** Data Types ** | [Spark Data Types](http://spark.apache.org/docs/latest/sql-ref-datatypes.html) |  [Hive Data Types](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types)

## References

1. [HiveServer2 Overview](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Overview)
