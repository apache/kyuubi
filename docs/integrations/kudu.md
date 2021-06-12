# Kyuubi On Apache Kudu

## What is Apache Kudu

> A new addition to the open source Apache Hadoop ecosystem, Apache Kudu completes Hadoop's storage layer to enable fast analytics on fast data.

When you are reading this documentation, we suppose that you are not necessary to be familiar with [Apache Kudu](https://kudu.apache.org/). But at least, you have one running Kudu cluster which is able to be connected for you. And it is even better for you to understand what Apache Kudu is capable with.

Anything missing on this page about Apache Kudu background knowledge, you can refer to its official website.

## Why Kyuubi on Kudu
Basically, Kyuubi can take place of HiveServer2 as a multi tenant ad-hoc SQL on Hadoop solution, with the advantages of speed and power coming from Spark SQL. You can run SQL queries towards both data source and Hive tables whose data is secured only with computing resources you are authorized.

> Spark SQL supports operating on a variety of data sources through the DataFrame interface. A DataFrame can be operated on using relational transformations and can also be used to create a temporary view. Registering a DataFrame as a temporary view allows you to run SQL queries over its data. This section describes the general methods for loading and saving data using the Spark Data Sources and then goes into specific options that are available for the built-in data sources.

In Kyuubi, we can register Kudu tables and other data source tables as Spark temporary views to enable federated union queries across Hive, Kudu, and other data sources.

## Kudu Integration with Apache Spark
Before integrating Kyuubi with Kudu, we strongly suggest that you integrate and test Spark with Kudu first. You may find the guide from Kudu's online documentation -- [Kudu Integration with Spark](https://kudu.apache.org/docs/developing.html#_kudu_integration_with_spark)

## Kudu Integration with Kyuubi

#### Install Kudu Spark Dependency
Confirm your Kudu cluster version and download the corresponding kudu spark dependency library, such as [org.apache.kudu:kudu-spark3_2.12-1.14.0](https://repo1.maven.org/maven2/org/apache/kudu/kudu-spark3_2.12/1.14.0/kudu-spark3_2.12-1.14.0.jar) to `$SPARK_HOME`/jars.

#### Start Kyuubi

Now, you can start Kyuubi server with this kudu embedded Spark distribution.

#### Start Beeline Or Other Client You Prefer

```shell
bin/beeline -u 'jdbc:hive2://<host>:<port>/;principal=<if kerberized>;#spark.yarn.queue=kyuubi_test'
```

#### Register Kudu table as Spark Temporary view

```sql
CREATE TEMPORARY VIEW kudutest
USING kudu
options ( 
  kudu.master "ip1:port1,ip2:port2,...",
  kudu.table "kudu::test.testtbl")
```

```sql
0: jdbc:hive2://spark5.jd.163.org:10009/> show tables;
19/07/09 15:28:03 INFO ExecuteStatementInClientMode: Running query 'show tables' with 1104328b-515c-4f8b-8a68-1c0b202bc9ed
19/07/09 15:28:03 INFO KyuubiSparkUtil$: Application application_1560304876299_3805060 has been activated
19/07/09 15:28:03 INFO ExecuteStatementInClientMode: Executing query in incremental mode, running 1 jobs before optimization
19/07/09 15:28:03 INFO ExecuteStatementInClientMode: Executing query in incremental mode, running 1 jobs without optimization
19/07/09 15:28:03 INFO DAGScheduler: Asked to cancel job group 1104328b-515c-4f8b-8a68-1c0b202bc9ed
+-----------+-----------------------------+--------------+--+
| database  |          tableName          | isTemporary  |
+-----------+-----------------------------+--------------+--+
| kyuubi    | hive_tbl                    | false        |
|           | kudutest                    | true         |
+-----------+-----------------------------+--------------+--+
2 rows selected (0.29 seconds)
```

#### Query Kudu Table

```sql
0: jdbc:hive2://spark5.jd.163.org:10009/> select * from kudutest;
19/07/09 15:25:17 INFO ExecuteStatementInClientMode: Running query 'select * from kudutest' with ac3e8553-0d79-4c57-add1-7d3ffe34ba16
19/07/09 15:25:17 INFO KyuubiSparkUtil$: Application application_1560304876299_3805060 has been activated
19/07/09 15:25:17 INFO ExecuteStatementInClientMode: Executing query in incremental mode, running 3 jobs before optimization
19/07/09 15:25:17 INFO ExecuteStatementInClientMode: Executing query in incremental mode, running 3 jobs without optimization
19/07/09 15:25:17 INFO DAGScheduler: Asked to cancel job group ac3e8553-0d79-4c57-add1-7d3ffe34ba16
+---------+---------------+----------------+--+
| userid  | sharesetting  | notifysetting  |
+---------+---------------+----------------+--+
| 1       | 1             | 1              |
| 5       | 5             | 5              |
| 2       | 2             | 2              |
| 3       | 3             | 3              |
| 4       | 4             | 4              |
+---------+---------------+----------------+--+
5 rows selected (1.083 seconds)
```


#### Join Kudu table with Hive table

```sql
0: jdbc:hive2://spark5.jd.163.org:10009/> select t1.*, t2.* from hive_tbl t1 join kudutest t2 on t1.userid=t2.userid+1;
19/07/09 15:31:01 INFO ExecuteStatementInClientMode: Running query 'select t1.*, t2.* from hive_tbl t1 join kudutest t2 on t1.userid=t2.userid+1' with 6982fa5c-29fa-49be-a5bf-54c935bbad18
19/07/09 15:31:01 INFO KyuubiSparkUtil$: Application application_1560304876299_3805060 has been activated
<omitted lines.... >
19/07/09 15:31:01 INFO DAGScheduler: Asked to cancel job group 6982fa5c-29fa-49be-a5bf-54c935bbad18
+---------+---------------+----------------+---------+---------------+----------------+--+
| userid  | sharesetting  | notifysetting  | userid  | sharesetting  | notifysetting  |
+---------+---------------+----------------+---------+---------------+----------------+--+
| 2       | 2             | 2              | 1       | 1             | 1              |
| 3       | 3             | 3              | 2       | 2             | 2              |
| 4       | 4             | 4              | 3       | 3             | 3              |
+---------+---------------+----------------+---------+---------------+----------------+--+
3 rows selected (1.63 seconds)
```

#### Insert to Kudu table

You should notice that only `INSERT INTO` is supported by Kudu, `OVERWITRE` data is not supported

```sql
0: jdbc:hive2://spark5.jd.163.org:10009/> insert overwrite table kudutest select *  from hive_tbl;
19/07/09 15:35:29 INFO ExecuteStatementInClientMode: Running query 'insert overwrite table kudutest select *  from hive_tbl' with 1afdb791-1aa7-4ceb-8ba8-ff53c17615d1
19/07/09 15:35:29 INFO KyuubiSparkUtil$: Application application_1560304876299_3805060 has been activated
19/07/09 15:35:30 ERROR ExecuteStatementInClientMode:
Error executing query as bdms_hzyaoqin,
insert overwrite table kudutest select *  from hive_tbl
Current operation state RUNNING,
java.lang.UnsupportedOperationException: overwrite is not yet supported
	at org.apache.kudu.spark.kudu.KuduRelation.insert(DefaultSource.scala:424)
	at org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand.run(InsertIntoDataSourceCommand.scala:42)
	at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult$lzycompute(commands.scala:70)
	at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult(commands.scala:68)
	at org.apache.spark.sql.execution.command.ExecutedCommandExec.executeCollect(commands.scala:79)
	at org.apache.spark.sql.Dataset$$anonfun$6.apply(Dataset.scala:190)
	at org.apache.spark.sql.Dataset$$anonfun$6.apply(Dataset.scala:190)
	at org.apache.spark.sql.Dataset$$anonfun$52.apply(Dataset.scala:3259)
	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:77)
	at org.apache.spark.sql.Dataset.withAction(Dataset.scala:3258)
	at org.apache.spark.sql.Dataset.<init>(Dataset.scala:190)
	at org.apache.spark.sql.Dataset$.ofRows(Dataset.scala:75)
	at org.apache.spark.sql.SparkSQLUtils$.toDataFrame(SparkSQLUtils.scala:39)
	at org.apache.kyuubi.operation.statement.ExecuteStatementInClientMode.execute(ExecuteStatementInClientMode.scala:152)
	at org.apache.kyuubi.operation.statement.ExecuteStatementOperation$$anon$1$$anon$2.run(ExecuteStatementOperation.scala:74)
	at org.apache.kyuubi.operation.statement.ExecuteStatementOperation$$anon$1$$anon$2.run(ExecuteStatementOperation.scala:70)
	at java.security.AccessController.doPrivileged(Native Method)
	at javax.security.auth.Subject.doAs(Subject.java:422)
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1698)
	at org.apache.kyuubi.operation.statement.ExecuteStatementOperation$$anon$1.run(ExecuteStatementOperation.scala:70)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
	at java.lang.Thread.run(Thread.java:745)


19/07/09 15:35:30 INFO DAGScheduler: Asked to cancel job group 1afdb791-1aa7-4ceb-8ba8-ff53c17615d1

```

```sql
0: jdbc:hive2://spark5.jd.163.org:10009/> insert into table kudutest select * from hive_tbl;
19/07/09 15:36:26 INFO ExecuteStatementInClientMode: Running query 'insert into table kudutest select *  from hive_tbl' with f7460400-0564-4f98-93b6-ad76e579e7af
19/07/09 15:36:26 INFO KyuubiSparkUtil$: Application application_1560304876299_3805060 has been activated
<omitted lines ...>
19/07/09 15:36:27 INFO DAGScheduler: ResultStage 36 (foreachPartition at KuduContext.scala:332) finished in 0.322 s
19/07/09 15:36:27 INFO DAGScheduler: Job 36 finished: foreachPartition at KuduContext.scala:332, took 0.324586 s
19/07/09 15:36:27 INFO KuduContext: completed upsert ops: duration histogram: 33.333333333333336%: 2ms, 66.66666666666667%: 64ms, 100.0%: 102ms, 100.0%: 102ms
19/07/09 15:36:27 INFO ExecuteStatementInClientMode: Executing query in incremental mode, running 1 jobs before optimization
19/07/09 15:36:27 INFO ExecuteStatementInClientMode: Executing query in incremental mode, running 1 jobs without optimization
19/07/09 15:36:27 INFO DAGScheduler: Asked to cancel job group f7460400-0564-4f98-93b6-ad76e579e7af
+---------+--+
| Result  |
+---------+--+
+---------+--+
No rows selected (0.611 seconds)
```

## References
[https://kudu.apache.org/](https://kudu.apache.org/)
[https://kudu.apache.org/docs/developing.html#_kudu_integration_with_spark](https://kudu.apache.org/docs/developing.html#_kudu_integration_with_spark)
[https://github.com/NetEase/kyuubi](https://github.com/NetEase/kyuubi)
[https://spark.apache.org/docs/latest/sql-data-sources.html](https://spark.apache.org/docs/latest/sql-data-sources.html)
