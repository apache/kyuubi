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

<script async defer src="https://buttons.github.io/buttons.js"></script>

<div align=center>

![](../imgs/kyuubi_logo.png)

</div>

# Getting Started with Apache Kyuubi


## Getting Kyuubi

Currently, Apache Kyuubi maintains all its releases on our official [website](https://kyuubi.apache.org/releases.html).
You can get the most recent stable release of Apache Kyuubi here:

<a class="github-button" href="https://kyuubi.apache.org/releases.html" data-color-scheme="no-preference: light; light: dark; dark: light;" data-icon="octicon-download" data-size="large" aria-label="Download Kyuubi">Download</a>

## Requirements

These are essential components required for Kyuubi to startup.
For quick start deployment, the only thing you need is `JAVA_HOME` and `SPARK_HOME` being correctly set.
The Kyuubi release package you downloaded or built contains the rest prerequisites inside already.

Components| Role | Optional | Version | Remarks
--- | --- | --- | --- | ---
Java | Java<br>Runtime<br>Environment | Required | Java 8/11 | Kyuubi is pre-built with Java 8
Spark | Distributed<br>SQL<br>Engine | Required | 3.0.0 and above | By default Kyuubi binary release is delivered without<br> a Spark tarball.
HDFS | Distributed<br>File<br>System |  Optional | referenced<br>by<br>Spark | Hadoop Distributed File System is a <br>part of Hadoop framework, used to<br> store and process the datasets.<br> You can interact with any<br> Spark-compatible versions of HDFS.
Hive | Metastore | Optional | referenced<br>by<br>Spark | Hive Metastore for Spark SQL to connect
Zookeeper | Service<br>Discovery | Optional | Any<br>zookeeper<br>ensemble<br>compatible<br>with<br>curator(2.12.0) | By default, Kyuubi provides a<br> embeded Zookeeper server inside for<br> non-production use.

Additionally, if you want to work with other Spark compatible systems or plugins, you only need to take care of them as using them with regular Spark applications.
For example, you can run Spark SQL engines created by the Kyuubi on any cluster manager, including YARN, Kubernetes, Mesos, e.t.c...
Or, you can manipulate data from different data sources with the Spark Datasource API, e.g. Delta Lake, Apache Hudi, Apache Iceberg, Apache Kudu and e.t.c...

## Installation

To install Kyuubi, you need to unpack the tarball. For example,

```bash
tar zxf apache-kyuubi-1.3.1-incubating-bin.tgz
```

This will result in the creation of a subdirectory named `apache-kyuubi-1.3.1-incubating-bin` shown below,

```bash
apache-kyuubi-1.3.1-incubating-bin
├── DISCLAIMER
├── LICENSE
├── NOTICE
├── RELEASE
├── bin
├── conf
|   ├── kyuubi-defaults.conf.template
│   ├── kyuubi-env.sh.template
│   └── log4j.properties.template
├── docker
│   ├── Dockerfile
│   ├── kyuubi-configmap.yaml
│   ├── kyuubi-pod.yaml
│   └── kyuubi-service.yaml
├── extension
│  └── kyuubi-extension-spark-3-1_2.12-1.3.1-incubating.jar
├── externals
│  └── engines
├── jars
├── licenses
├── logs
├── pid
└── work
```

From top to bottom are:

- DISCLAIMER: the disclaimer made by Apache Kyuubi Community as a project still in ASF Incubator.
- LICENSE: the [APACHE LICENSE, VERSION 2.0](https://www.apache.org/licenses/LICENSE-2.0) we claim to obey.
- RELEASE: the build information of this package.
- NOTICE: the natice made by Apache Kyuubi Community about its project and dependencies.
- bin: the entry of the Kyuubi server with `kyuubi` as the startup script.
- conf: all the defaults used by Kyuubi Server itself or creating a session with Spark applications.
- externals
  - engines: contains all kinds of SQL engines that we support, e.g. Apache Spark, Apache Flink(coming soon).
- licenses: a bunch of licenses included
- jars: packages needed by the Kyuubi server.
- logs: Where the logs of the Kyuubi server locates.
- pid: stores the PID file of the Kyuubi server instance.
- work: the root of the working directories of all the forked sub-processes, a.k.a. SQL engines.

## Running Kyuubi

As mentioned above, for a quick start deployment, then only you need to be sure is that your java runtime environment and `SPARK_HOME` are correct.

### Setup JAVA

You can either set it system-widely,  e.g. in the `.bashrc` file.

```bash
java -version
java version "1.8.0_251"
Java(TM) SE Runtime Environment (build 1.8.0_251-b08)
Java HotSpot(TM) 64-Bit Server VM (build 25.251-b08, mixed mode)
```

Or, `export JAVA_HOME=/path/to/java` in the local os session.

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-11.0.5.jdk/Contents/Home
 java -version
java version "11.0.5" 2019-10-15 LTS
Java(TM) SE Runtime Environment 18.9 (build 11.0.5+10-LTS)
Java HotSpot(TM) 64-Bit Server VM 18.9 (build 11.0.5+10-LTS, mixed mode)
```

The recommended place to set `JAVA_HOME` is `$KYUUBI_HOME/conf/kyuubi-env.sh`, as the ways above are too flaky.
The `JAVA_HOME` in `$KYUUBI_HOME/conf/kyuubi-env.sh` will take others' precedence.

### Setup Spark

Similar to `JAVA_HOME`, you can also set `SPARK_HOME` in different ways. However, we recommend setting it in `$KYUUBI_HOME/conf/kyuubi-env.sh` too.

For example,

```bash
SPARK_HOME=~/Downloads/spark-3.2.0-bin-hadoop3.2
```

### Starting Kyuubi

```bash
bin/kyuubi start
```

It will print all essential environment variables on the screen during the server starts, and you may check whether they are expected.

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

If all goes well, this will result in the creation of the Kyuubi server instance with a `PID` stored in `$KYUUBI_HOME/pid/kyuubi-<username>-org.apache.kyuubi.server.KyuubiServer.pid`

Then, you can get the JDBC connection URL at the end of the log file, e.g.

```
ThriftFrontendService: Starting and exposing JDBC connection at: jdbc:hive2://localhost:10009/
```

If something goes wrong, you shall be able to find some clues in the log file too.

Alternatively, it can run in the foreground, with the logs and other output written to stdout/stderr.
Both streams should be captured if using a supervision system like `supervisord`.

```bash
bin/kyuubi run
```

## Using Hive Beeline

Kyuubi server is compatible with Apache Hive beeline, so you can use `$SPARK_HOME/bin/beeline` for testing.

### Opening a Connection

The command below will tell the Kyuubi server to create a session with itself.

```logtalk
bin/beeline -u 'jdbc:hive2://localhost:10009/'
Connecting to jdbc:hive2://localhost:10009/
Connected to: Spark SQL (version 1.0.2)
Driver: Hive JDBC (version 2.3.7)
Transaction isolation: TRANSACTION_REPEATABLE_READ
Beeline version 2.3.7 by Apache Hive
0: jdbc:hive2://localhost:10009/>
```

In this case, the session will create for the user named 'anonymous'.

Kyuubi will create a Spark SQL engine application using `kyuubi-spark-sql-engine_2.12-<version>.jar`.
It will cost awhile for the application to be ready before fully establishing the session.
Otherwise, an existing application will be resued, and the time cost here is negligible.

Similarly, you can create a session for another user(or principal, subject, and maybe something else you defined), e.g. named `kentyao`,

```bash
bin/beeline -u 'jdbc:hive2://localhost:10009/' -n kentyao
```

The formerly created Spark application for user 'anonymous' will not be reused in this case, while a brand new application will be submitted for user 'kentyao' instead.

Then, you can see 3 processes running in your local environment, including one `KyuubiServer` instance and 2 `SparkSubmit` instances as the SQL engines.

```
75730 Jps
70843 KyuubiServer
72566 SparkSubmit
75356 SparkSubmit
```

### Execute Statements

If the beeline session is successfully connected, then you can run any query supported by Spark SQL now. For example,

```logtalk
0: jdbc:hive2://10.242.189.214:2181/> select timestamp '2018-11-17';
2021-10-28 13:56:27.509 INFO operation.ExecuteStatement: Processing kent's query[1f619182-20ad-4733-995b-a5e43b80d998]: INITIALIZED_STATE -> PENDING_STATE, statement: select timestamp '2018-11-17'
2021-10-28 13:56:27.547 INFO operation.ExecuteStatement: Processing kent's query[1f619182-20ad-4733-995b-a5e43b80d998]: PENDING_STATE -> RUNNING_STATE, statement: select timestamp '2018-11-17'
2021-10-28 13:56:27.540 INFO operation.ExecuteStatement: Processing kent's query[a46ca504-fe3a-4dfb-be1e-19770af8ac4c]: INITIALIZED_STATE -> PENDING_STATE, statement: select timestamp '2018-11-17'
2021-10-28 13:56:27.541 INFO operation.ExecuteStatement: Processing kent's query[a46ca504-fe3a-4dfb-be1e-19770af8ac4c]: PENDING_STATE -> RUNNING_STATE, statement: select timestamp '2018-11-17'
2021-10-28 13:56:27.543 INFO operation.ExecuteStatement:
           Spark application name: kyuubi_USER_kent_7ad055d0-3eca-4b78-87e8-94b22f3bade9
                 application ID: local-1635400506190
                 application web UI: http://10.242.189.214:56774
                 master: local[*]
                 deploy mode: client
                 version: 3.2.0
           Start time: 2021-10-28T13:55:05.528
           User: kent
2021-10-28 13:56:27.604 INFO operation.ExecuteStatement: Processing kent's query[a46ca504-fe3a-4dfb-be1e-19770af8ac4c]: RUNNING_STATE -> RUNNING_STATE, statement: select timestamp '2018-11-17'
2021-10-28 13:56:27.627 INFO codegen.CodeGenerator: Code generated in 6.696179 ms
2021-10-28 13:56:27.635 INFO spark.SparkContext: Starting job: collect at ExecuteStatement.scala:97
2021-10-28 13:56:27.639 INFO kyuubi.SQLOperationListener: Query [a46ca504-fe3a-4dfb-be1e-19770af8ac4c]: Job 3 started with 1 stages, 1 active jobs running
2021-10-28 13:56:27.639 INFO kyuubi.SQLOperationListener: Query [a46ca504-fe3a-4dfb-be1e-19770af8ac4c]: Stage 3 started with 1 tasks, 1 active stages running
2021-10-28 13:56:27.651 INFO scheduler.DAGScheduler: Job 3 finished: collect at ExecuteStatement.scala:97, took 0.016234 s
2021-10-28 13:56:27.653 INFO kyuubi.SQLOperationListener: Finished stage: Stage(3, 0); Name: 'collect at ExecuteStatement.scala:97'; Status: succeeded; numTasks: 1; Took: 13 msec
2021-10-28 13:56:27.663 INFO scheduler.StatsReportListener: task runtime:(count: 1, mean: 8.000000, stdev: 0.000000, max: 8.000000, min: 8.000000)
2021-10-28 13:56:27.664 INFO scheduler.StatsReportListener: 	0%	5%	10%	25%	50%	75%	90%	95%	100%
2021-10-28 13:56:27.664 INFO scheduler.StatsReportListener: 	8.0 ms	8.0 ms	8.0 ms	8.0 ms	8.0 ms	8.0 ms	8.0 ms	8.0 ms	8.0 ms
2021-10-28 13:56:27.665 INFO scheduler.StatsReportListener: shuffle bytes written:(count: 1, mean: 0.000000, stdev: 0.000000, max: 0.000000, min: 0.000000)
2021-10-28 13:56:27.665 INFO scheduler.StatsReportListener: 	0%	5%	10%	25%	50%	75%	90%	95%	100%
2021-10-28 13:56:27.665 INFO scheduler.StatsReportListener: 	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B
2021-10-28 13:56:27.666 INFO scheduler.StatsReportListener: fetch wait time:(count: 1, mean: 0.000000, stdev: 0.000000, max: 0.000000, min: 0.000000)
2021-10-28 13:56:27.666 INFO scheduler.StatsReportListener: 	0%	5%	10%	25%	50%	75%	90%	95%	100%
2021-10-28 13:56:27.666 INFO scheduler.StatsReportListener: 	0.0 ms	0.0 ms	0.0 ms	0.0 ms	0.0 ms	0.0 ms	0.0 ms	0.0 ms	0.0 ms
2021-10-28 13:56:27.667 INFO scheduler.StatsReportListener: remote bytes read:(count: 1, mean: 0.000000, stdev: 0.000000, max: 0.000000, min: 0.000000)
2021-10-28 13:56:27.667 INFO scheduler.StatsReportListener: 	0%	5%	10%	25%	50%	75%	90%	95%	100%
2021-10-28 13:56:27.667 INFO scheduler.StatsReportListener: 	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B	0.0 B
2021-10-28 13:56:27.668 INFO scheduler.StatsReportListener: task result size:(count: 1, mean: 1402.000000, stdev: 0.000000, max: 1402.000000, min: 1402.000000)
2021-10-28 13:56:27.668 INFO scheduler.StatsReportListener: 	0%	5%	10%	25%	50%	75%	90%	95%	100%
2021-10-28 13:56:27.669 INFO scheduler.StatsReportListener: 	1402.0 B	1402.0 B	1402.0 B	1402.0 B	1402.0 B	1402.0 B	1402.0 B	1402.0 B	1402.0 B
2021-10-28 13:56:27.669 INFO codegen.CodeGenerator: Code generated in 8.815996 ms
2021-10-28 13:56:27.672 INFO scheduler.StatsReportListener: executor (non-fetch) time pct: (count: 1, mean: 12.500000, stdev: 0.000000, max: 12.500000, min: 12.500000)
2021-10-28 13:56:27.672 INFO scheduler.StatsReportListener: 	0%	5%	10%	25%	50%	75%	90%	95%	100%
2021-10-28 13:56:27.672 INFO scheduler.StatsReportListener: 	13 %	13 %	13 %	13 %	13 %	13 %	13 %	13 %	13 %
2021-10-28 13:56:27.673 INFO scheduler.StatsReportListener: fetch wait time pct: (count: 1, mean: 0.000000, stdev: 0.000000, max: 0.000000, min: 0.000000)
2021-10-28 13:56:27.673 INFO scheduler.StatsReportListener: 	0%	5%	10%	25%	50%	75%	90%	95%	100%
2021-10-28 13:56:27.673 INFO scheduler.StatsReportListener: 	 0 %	 0 %	 0 %	 0 %	 0 %	 0 %	 0 %	 0 %	 0 %
2021-10-28 13:56:27.674 INFO scheduler.StatsReportListener: other time pct: (count: 1, mean: 87.500000, stdev: 0.000000, max: 87.500000, min: 87.500000)
2021-10-28 13:56:27.674 INFO scheduler.StatsReportListener: 	0%	5%	10%	25%	50%	75%	90%	95%	100%
2021-10-28 13:56:27.674 INFO scheduler.StatsReportListener: 	88 %	88 %	88 %	88 %	88 %	88 %	88 %	88 %	88 %
2021-10-28 13:56:27.674 INFO kyuubi.SQLOperationListener: Query [a46ca504-fe3a-4dfb-be1e-19770af8ac4c]: Job 3 succeeded, 0 active jobs running
2021-10-28 13:56:27.744 INFO operation.ExecuteStatement: Processing kent's query[a46ca504-fe3a-4dfb-be1e-19770af8ac4c]: RUNNING_STATE -> FINISHED_STATE, statement: select timestamp '2018-11-17', time taken: 0.202 seconds
2021-10-28 13:56:27.784 INFO operation.ExecuteStatement: Query[1f619182-20ad-4733-995b-a5e43b80d998] in FINISHED_STATE
2021-10-28 13:56:27.784 INFO operation.ExecuteStatement: Processing kent's query[1f619182-20ad-4733-995b-a5e43b80d998]: RUNNING_STATE -> FINISHED_STATE, statement: select timestamp '2018-11-17', time taken: 0.237 seconds
+----------------------------------+
| TIMESTAMP '2018-11-17 00:00:00'  |
+----------------------------------+
| 2018-11-17 00:00:00.0            |
+----------------------------------+
1 row selected (0.404 seconds)
```

As shown in the above case, you can retrieve all the operation logs, the result schema, and the result to your client-side in the beeline console.

Additionally, some useful information about the background Spark SQL application associated with this connection is also printed in the operation log.
For example, you can get the Spark web UI from the log for debugging or tuning.

![](../imgs/spark_jobs_page.png)

### Closing a Connection

Close the session between beeline and Kyuubi server by executing `!quit`, for example,

```bash
0: jdbc:hive2://localhost:10009/> !quit
Closing: 0: jdbc:hive2://localhost:10009/
```

## Stopping Kyuubi

Stop Kyuubi by running the following in the `$KYUUBI_HOME` directory:

```bash
bin/kyuubi.sh stop
```

And then, you will see the KyuubiServer waving goodbye to you.

```logtalk
Stopping org.apache.kyuubi.server.KyuubiServer
  __  __                           __
 /\ \/\ \                         /\ \      __
 \ \ \/'/'  __  __  __  __  __  __\ \ \____/\_\
  \ \ , <  /\ \/\ \/\ \/\ \/\ \/\ \\ \ '__`\/\ \
   \ \ \\`\\ \ \_\ \ \ \_\ \ \ \_\ \\ \ \L\ \ \ \
    \ \_\ \_\/`____ \ \____/\ \____/ \ \_,__/\ \_\
     \/_/\/_/`/___/> \/___/  \/___/   \/___/  \/_/
                /\___/
                \/__/
Bye!
```

The `KyuubiServer` instance will be stopped immediately while the SQL engine's application will still be alive for a while.

If you start Kyuubi again before the SQL engine application terminates itself, it will reconnect to the newly created `KyuubiServer` instance.
