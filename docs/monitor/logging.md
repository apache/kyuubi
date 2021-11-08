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

<div align=center>

![](../imgs/kyuubi_logo.png)

</div>

# Monitoring Kyuubi - Logging System

Kyuubi uses [Apache Log4j](https://logging.apache.org/) for logging.

In general, there are mainly three components in the Kyuubi architecture that will produce component-oriented logs to help you trace breadcrumbs for SQL workloads against Kyuubi.

- Logs of Kyuubi Server
- Logs of Kyuubi Engines
- Operation logs

In addition, a Kyuubi deployment for production usually relies on some other external systems.
For example, both Kyuubi servers and engines will use [Apache Zookeeper](https://zookeeper.apache.org/) for service discovery.
The instructions for external system loggings will not be included in this article.

## Logs of Kyuubi Server

Logs of Kyuubi Server show us the activities of the server instance including how start/stop, how does it response client requests, etc.

### Configuring Server Logging

#### Basic Configurations

You can configure it by adding a `log4j.properties` file in the `$KYUUBI_HOME/conf` directory.
One way to start is to make a copy of the existing `log4j.properties.template` located there.

For example,

```shell
# cd $KYUUBI_HOME
cp conf/log4j.properties.template conf/log4j.properties
```

With or without the above step, by default the server logging will redirect the logs to a file named `kyuubi-${env:USER}-org.apache.kyuubi.server.KyuubiServer-${env:HOSTNAME}.out` under the directory of `$KYUUBI_HOME/logs`.

For example, you can easily find where the server log goes when staring a Kyuubi server from the console output.

```shell
$ export SPARK_HOME=/Users/kentyao/Downloads/spark/spark-3.2.0-bin-hadoop3.2
$ cd ~/svn-kyuubi/v1.3.1-incubating-rc0/apache-kyuubi-1.3.1-incubating-bin
$ bin/kyuubi start
```

```log
Starting Kyuubi Server from /Users/kentyao/svn-kyuubi/v1.3.1-incubating-rc0/apache-kyuubi-1.3.1-incubating-bin
Warn: Not find kyuubi environment file /Users/kentyao/svn-kyuubi/v1.3.1-incubating-rc0/apache-kyuubi-1.3.1-incubating-bin/conf/kyuubi-env.sh, using default ones...
JAVA_HOME: /Library/Java/JavaVirtualMachines/jdk1.8.0_251.jdk/Contents/Home
KYUUBI_HOME: /Users/kentyao/svn-kyuubi/v1.3.1-incubating-rc0/apache-kyuubi-1.3.1-incubating-bin
KYUUBI_CONF_DIR: /Users/kentyao/svn-kyuubi/v1.3.1-incubating-rc0/apache-kyuubi-1.3.1-incubating-bin/conf
KYUUBI_LOG_DIR: /Users/kentyao/svn-kyuubi/v1.3.1-incubating-rc0/apache-kyuubi-1.3.1-incubating-bin/logs
KYUUBI_PID_DIR: /Users/kentyao/svn-kyuubi/v1.3.1-incubating-rc0/apache-kyuubi-1.3.1-incubating-bin/pid
KYUUBI_WORK_DIR_ROOT: /Users/kentyao/svn-kyuubi/v1.3.1-incubating-rc0/apache-kyuubi-1.3.1-incubating-bin/work
SPARK_HOME: /Users/kentyao/Downloads/spark/spark-3.2.0-bin-hadoop3.2
SPARK_CONF_DIR: /Users/kentyao/Downloads/spark/spark-3.2.0-bin-hadoop3.2/conf
HADOOP_CONF_DIR:
Starting org.apache.kyuubi.server.KyuubiServer, logging to /Users/kentyao/svn-kyuubi/v1.3.1-incubating-rc0/apache-kyuubi-1.3.1-incubating-bin/logs/kyuubi-kentyao-org.apache.kyuubi.server.KyuubiServer-hulk.local.out
Welcome to
  __  __                           __
 /\ \/\ \                         /\ \      __
 \ \ \/'/'  __  __  __  __  __  __\ \ \____/\_\
  \ \ , <  /\ \/\ \/\ \/\ \/\ \/\ \\ \ '__`\/\ \
   \ \ \\`\\ \ \_\ \ \ \_\ \ \ \_\ \\ \ \L\ \ \ \
    \ \_\ \_\/`____ \ \____/\ \____/ \ \_,__/\ \_\
     \/_/\/_/`/___/> \/___/  \/___/   \/___/  \/_/
                /\___/
                \/__/
```

#### KYUUBI_LOG_DIR

You may also notice that there is an environment variable called `KYUUBI_LOG_DIR` in the above example.

`KYUUBI_LOG_DIR` determines which folder we want to put our server log files.

For example, the below command will locate the log files to `/Users/kentyao/tmp`.

```shell
$ mkdir /Users/kentyao/tmp
$ KYUUBI_LOG_DIR=/Users/kentyao/tmp bin/kyuubi start
```

```log
Starting org.apache.kyuubi.server.KyuubiServer, logging to /Users/kentyao/tmp/kyuubi-kentyao-org.apache.kyuubi.server.KyuubiServer-hulk.local.out
```

#### KYUUBI_MAX_LOG_FILES

`KYUUBI_MAX_LOG_FILES` controls how many log files will be remained after a Kyuubi server reboots.

#### Custom Log4j Settings

Taking control of `$KYUUBI_HOME/conf/log4j.properties` will also give us the ability of customizing server logging as we want.

For example, we can disable the console appender and enable the file appender like,

```properties
log4j.rootCategory=INFO, FA
log4j.appender.FA=org.apache.log4j.FileAppender
log4j.appender.FA.append=false
log4j.appender.FA.file=log/dummy.log
log4j.appender.FA.layout=org.apache.log4j.PatternLayout
log4j.appender.FA.layout.ConversionPattern=%d{HH:mm:ss.SSS} %t %p %c{2}: %m%n
log4j.appender.FA.Threshold=DEBUG
```

Then everything goes to `log/dummy.log`.

## Logs of Spark SQL Engine

Spark SQL Engine is one type of Kyuubi Engines and also a typical Spark application.
Thus, its logs mainly contain the logs of a Spark Driver.
Meanwhile, it also includes how all the services of an engine start/stop, how does it response the incoming calls from Kyuubi servers, etc.

In general, when an exception occurs, we are able to find more information and clues in the engine's logs.

#### Configuring Engine Logging

Please refer to Apache Spark online documentation -[Configuring Logging](https://spark.apache.org/docs/latest/configuration.html#configuring-logging) for instructions.

#### Where to Find the Engine Log

The engine logs locate differently based on the deploy mode and the cluster manager.
When using local backend or `client` deploy mode for other cluster managers, such as YARN, you can find the whole engine log in `$KYUUBI_WORK_DIR_ROOT/${session username}/kyuubi-spark-sql-engine.log.${num}`.
Different session users have different folders to group all live and historical engine logs.
Each engine will have one and only engine log.
When using `cluster` deploy mode, the local engine logs only contain very little information, the main parts of engine logs are on the remote driver side, e.g. for YARN cluster, they are in ApplicationMasters' log.

## Operation Logs

Operation log will show how SQL queries are executed, such as query planning, execution, and statistic reports.

Operation logs can reveal directly to end-users how their queries are being executed on the server/engine-side, including some process-oriented information, and why their queries are slow or in error.

For example, when you, as an end-user, use `beeline` to connect a Kyuubi server and execute query like below.

```shell
bin/beeline -u 'jdbc:hive2://10.242.189.214:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=kyuubi' -n kent -e 'select * from src;'
```

You will both get the final results and the corresponding operation logs telling you the journey of the query.

```log
0: jdbc:hive2://10.242.189.214:2181/> select * from src;
2021-10-27 17:00:19.399 INFO operation.ExecuteStatement: Processing kent's query[fb5f57d2-2b50-4a46-961b-3a5c6a2d2597]: INITIALIZED_STATE -> PENDING_STATE, statement: select * from src
2021-10-27 17:00:19.401 INFO operation.ExecuteStatement: Processing kent's query[fb5f57d2-2b50-4a46-961b-3a5c6a2d2597]: PENDING_STATE -> RUNNING_STATE, statement: select * from src
2021-10-27 17:00:19.400 INFO operation.ExecuteStatement: Processing kent's query[26e169a2-6c06-450a-b758-e577ac673d70]: INITIALIZED_STATE -> PENDING_STATE, statement: select * from src
2021-10-27 17:00:19.401 INFO operation.ExecuteStatement: Processing kent's query[26e169a2-6c06-450a-b758-e577ac673d70]: PENDING_STATE -> RUNNING_STATE, statement: select * from src
2021-10-27 17:00:19.402 INFO operation.ExecuteStatement:
           Spark application name: kyuubi_USER_kent_6d4b5e53-ddd2-420c-b04f-326fb2b17e18
                 application ID: local-1635318669122
                 application web UI: http://10.242.189.214:50250
                 master: local[*]
                 deploy mode: client
                 version: 3.2.0
           Start time: 2021-10-27T15:11:08.416
           User: kent
2021-10-27 17:00:19.408 INFO metastore.HiveMetaStore: 6: get_database: default
2021-10-27 17:00:19.408 INFO HiveMetaStore.audit: ugi=kent	ip=unknown-ip-addr	cmd=get_database: default
2021-10-27 17:00:19.424 WARN conf.HiveConf: HiveConf of name hive.internal.ss.authz.settings.applied.marker does not exist
2021-10-27 17:00:19.424 WARN conf.HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
2021-10-27 17:00:19.424 WARN conf.HiveConf: HiveConf of name hive.stats.retries.wait does not exist
2021-10-27 17:00:19.424 INFO metastore.HiveMetaStore: 6: Opening raw store with implementation class:org.apache.hadoop.hive.metastore.ObjectStore
2021-10-27 17:00:19.425 INFO metastore.ObjectStore: ObjectStore, initialize called
2021-10-27 17:00:19.430 INFO metastore.MetaStoreDirectSql: Using direct SQL, underlying DB is DERBY
2021-10-27 17:00:19.431 INFO metastore.ObjectStore: Initialized ObjectStore
2021-10-27 17:00:19.434 INFO metastore.HiveMetaStore: 6: get_table : db=default tbl=src
2021-10-27 17:00:19.434 INFO HiveMetaStore.audit: ugi=kent	ip=unknown-ip-addr	cmd=get_table : db=default tbl=src
2021-10-27 17:00:19.449 INFO metastore.HiveMetaStore: 6: get_table : db=default tbl=src
2021-10-27 17:00:19.450 INFO HiveMetaStore.audit: ugi=kent	ip=unknown-ip-addr	cmd=get_table : db=default tbl=src
2021-10-27 17:00:19.510 INFO operation.ExecuteStatement: Processing kent's query[26e169a2-6c06-450a-b758-e577ac673d70]: RUNNING_STATE -> RUNNING_STATE, statement: select * from src
2021-10-27 17:00:19.544 INFO memory.MemoryStore: Block broadcast_5 stored as values in memory (estimated size 343.6 KiB, free 408.6 MiB)
2021-10-27 17:00:19.558 INFO memory.MemoryStore: Block broadcast_5_piece0 stored as bytes in memory (estimated size 33.5 KiB, free 408.5 MiB)
2021-10-27 17:00:19.559 INFO spark.SparkContext: Created broadcast 5 from
2021-10-27 17:00:19.600 INFO mapred.FileInputFormat: Total input files to process : 1
2021-10-27 17:00:19.627 INFO spark.SparkContext: Starting job: collect at ExecuteStatement.scala:97
2021-10-27 17:00:19.629 INFO kyuubi.SQLOperationListener: Query [26e169a2-6c06-450a-b758-e577ac673d70]: Job 5 started with 1 stages, 1 active jobs running
2021-10-27 17:00:19.631 INFO kyuubi.SQLOperationListener: Query [26e169a2-6c06-450a-b758-e577ac673d70]: Stage 5 started with 1 tasks, 1 active stages running
2021-10-27 17:00:19.713 INFO kyuubi.SQLOperationListener: Finished stage: Stage(5, 0); Name: 'collect at ExecuteStatement.scala:97'; Status: succeeded; numTasks: 1; Took: 83 msec
2021-10-27 17:00:19.713 INFO scheduler.DAGScheduler: Job 5 finished: collect at ExecuteStatement.scala:97, took 0.085454 s
2021-10-27 17:00:19.713 INFO scheduler.StatsReportListener: task runtime:(count: 1, mean: 78.000000, stdev: 0.000000, max: 78.000000, min: 78.000000)
2021-10-27 17:00:19.713 INFO scheduler.StatsReportListener: 	0%	5%	10%	25%	50%	75%	90%	95%	100%
2021-10-27 17:00:19.713 INFO scheduler.StatsReportListener: 	78.0 ms	78.0 ms	78.0 ms	78.0 ms	78.0 ms	78.0 ms	78.0 ms	78.0 ms	78.0 ms
2021-10-27 17:00:19.714 INFO scheduler.StatsReportListener: shuffle bytes written:(count: 1, mean: 0.000000, stdev: 0.000000, max: 0.000000, min: 0.000000)
2021-10-27 17:00:19.714 INFO scheduler.StatsReportListener: 	0%	5%	10%	25%	50%	75%	90%	95%	100%
2021-10-27 17:00:19.714 INFO scheduler.StatsReportListener: 	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B
2021-10-27 17:00:19.714 INFO scheduler.StatsReportListener: fetch wait time:(count: 1, mean: 0.000000, stdev: 0.000000, max: 0.000000, min: 0.000000)
2021-10-27 17:00:19.714 INFO scheduler.StatsReportListener: 	0%	5%	10%	25%	50%	75%	90%	95%	100%
2021-10-27 17:00:19.714 INFO scheduler.StatsReportListener: 	0.0 ms	0.0 ms	0.0 ms	0.0 ms	0.0 ms	0.0 ms	0.0 ms	0.0 ms	0.0 ms
2021-10-27 17:00:19.715 INFO scheduler.StatsReportListener: remote bytes read:(count: 1, mean: 0.000000, stdev: 0.000000, max: 0.000000, min: 0.000000)
2021-10-27 17:00:19.715 INFO scheduler.StatsReportListener: 	0%	5%	10%	25%	50%	75%	90%	95%	100%
2021-10-27 17:00:19.715 INFO scheduler.StatsReportListener: 	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B
2021-10-27 17:00:19.715 INFO scheduler.StatsReportListener: task result size:(count: 1, mean: 1471.000000, stdev: 0.000000, max: 1471.000000, min: 1471.000000)
2021-10-27 17:00:19.715 INFO scheduler.StatsReportListener: 	0%	5%	10%	25%	50%	75%	90%	95%	100%
2021-10-27 17:00:19.715 INFO scheduler.StatsReportListener: 	1471.0 B	1471.0 B	1471.0 B	1471.0 B	1471.0 B	1471.0 B	1471.0 B	1471.0 B	1471.0 B
2021-10-27 17:00:19.717 INFO scheduler.StatsReportListener: executor (non-fetch) time pct: (count: 1, mean: 61.538462, stdev: 0.000000, max: 61.538462, min: 61.538462)
2021-10-27 17:00:19.717 INFO scheduler.StatsReportListener: 	0%	5%	10%	25%	50%	75%	90%	95%	100%
2021-10-27 17:00:19.717 INFO scheduler.StatsReportListener: 	62 %	62 %	62 %	62 %	62 %	62 %	62 %	62 %	62 %
2021-10-27 17:00:19.718 INFO scheduler.StatsReportListener: fetch wait time pct: (count: 1, mean: 0.000000, stdev: 0.000000, max: 0.000000, min: 0.000000)
2021-10-27 17:00:19.718 INFO scheduler.StatsReportListener: 	0%	5%	10%	25%	50%	75%	90%	95%	100%
2021-10-27 17:00:19.718 INFO scheduler.StatsReportListener: 	 0 %	 0 %	 0 %	 0 %	 0 %	 0 %	 0 %	 0 %	 0 %
2021-10-27 17:00:19.718 INFO scheduler.StatsReportListener: other time pct: (count: 1, mean: 38.461538, stdev: 0.000000, max: 38.461538, min: 38.461538)
2021-10-27 17:00:19.718 INFO scheduler.StatsReportListener: 	0%	5%	10%	25%	50%	75%	90%	95%	100%
2021-10-27 17:00:19.718 INFO scheduler.StatsReportListener: 	38 %	38 %	38 %	38 %	38 %	38 %	38 %	38 %	38 %
2021-10-27 17:00:19.719 INFO kyuubi.SQLOperationListener: Query [26e169a2-6c06-450a-b758-e577ac673d70]: Job 5 succeeded, 0 active jobs running
2021-10-27 17:00:19.728 INFO codegen.CodeGenerator: Code generated in 12.277091 ms
2021-10-27 17:00:19.729 INFO operation.ExecuteStatement: Processing kent's query[26e169a2-6c06-450a-b758-e577ac673d70]: RUNNING_STATE -> FINISHED_STATE, statement: select * from src, time taken: 0.328 seconds
2021-10-27 17:00:19.731 INFO operation.ExecuteStatement: Query[fb5f57d2-2b50-4a46-961b-3a5c6a2d2597] in FINISHED_STATE
2021-10-27 17:00:19.731 INFO operation.ExecuteStatement: Processing kent's query[fb5f57d2-2b50-4a46-961b-3a5c6a2d2597]: RUNNING_STATE -> FINISHED_STATE, statement: select * from src, time taken: 0.33 seconds
+-------------------------------------------------+--------------------+
|                    version()                    | DATE '2021-10-27'  |
+-------------------------------------------------+--------------------+
| 3.2.0 5d45a415f3a29898d92380380cfd82bfc7f579ea  | 2021-10-27         |
+-------------------------------------------------+--------------------+
1 row selected (0.341 seconds)
```
## Further Readings

- [Monitoring Kyuubi - Events System](events.md)
- [Monitoring Kyuubi - Server Metrics](metrics.md)
- [Trouble Shooting](trouble_shooting.md)
- Spark Online Documentation
    - [Monitoring and Instrumentation](http://spark.apache.org/docs/latest/monitoring.html)
