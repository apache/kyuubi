<div align=center>

![](../imgs/kyuubi_logo.png)

</div>

# Integration with Hive Metastore

In this section, you will learn how to configure Kyuubi to interact with Hive Metastore.

- A common Hive metastore server could be set at Kyuubi server side
- Individual Hive metastore servers could be used for end users to set

## Requirements

- A running Hive metastore server
  - [Hive Metastore Administration](https://cwiki.apache.org/confluence/display/Hive/AdminManual+Metastore+Administration)
  - [Configuring the Hive Metastore for CDH](https://docs.cloudera.com/documentation/enterprise/latest/topics/cdh_ig_hive_metastore_configure.html)
- A Spark binary distribution built with `-Phive` support
  - Use the built in one in the Kyuubi distribution
  - Download from [Spark official website](https://spark.apache.org/downloads.html)
  - Build from Spark source, [Building With Hive and JDBC Support](http://spark.apache.org/docs/latest/building-spark.html#building-with-hive-and-jdbc-support)
- A copy of Hive client configuration

So the whole thing here is to let Spark applications use this copy of Hive configuration to start a Hive metastore client for their own to talk to the Hive metastore server.

## Default Behavior

By default, Kyuubi launches Spark SQL engines pointing to a dummy embedded [Apache Derby](https://db.apache.org/derby/)-based metastore for each application,
and this metadata can only be seen by one user at a time, e.g.

```shell script
bin/beeline -u 'jdbc:hive2://localhost:10009/' -n kentyao
Connecting to jdbc:hive2://localhost:10009/
Connected to: Spark SQL (version 1.0.0-SNAPSHOT)
Driver: Hive JDBC (version 2.3.7)
Transaction isolation: TRANSACTION_REPEATABLE_READ
Beeline version 2.3.7 by Apache Hive
0: jdbc:hive2://localhost:10009/> show databases;
2020-11-16 23:50:50.388 INFO operation.ExecuteStatement:
           Spark application name: kyuubi_kentyao_spark_2020-11-16T15:50:08.968Z
                 application ID:  local-1605541809797
                 application web UI: http://192.168.1.14:60165
                 master: local[*]
                 deploy mode: client
                 version: 3.0.1
           Start time: 2020-11-16T15:50:09.123Z
           User: kentyao
2020-11-16 23:50:50.404 INFO metastore.HiveMetaStore: 2: get_databases: *
2020-11-16 23:50:50.404 INFO HiveMetaStore.audit: ugi=kentyao	ip=unknown-ip-addr	cmd=get_databases: *
2020-11-16 23:50:50.423 INFO operation.ExecuteStatement: Processing kentyao's query[8453e657-c1c4-4391-8406-ab4747a66c45]: RUNNING_STATE -> FINISHED_STATE, statement: show databases, time taken: 0.035 seconds
+------------+
| namespace  |
+------------+
| default    |
+------------+
1 row selected (0.122 seconds)
0: jdbc:hive2://localhost:10009/> show tables;
2020-11-16 23:50:52.957 INFO operation.ExecuteStatement:
           Spark application name: kyuubi_kentyao_spark_2020-11-16T15:50:08.968Z
                 application ID:  local-1605541809797
                 application web UI: http://192.168.1.14:60165
                 master: local[*]
                 deploy mode: client
                 version: 3.0.1
           Start time: 2020-11-16T15:50:09.123Z
           User: kentyao
2020-11-16 23:50:52.968 INFO metastore.HiveMetaStore: 2: get_database: default
2020-11-16 23:50:52.968 INFO HiveMetaStore.audit: ugi=kentyao	ip=unknown-ip-addr	cmd=get_database: default
2020-11-16 23:50:52.970 INFO metastore.HiveMetaStore: 2: get_database: default
2020-11-16 23:50:52.970 INFO HiveMetaStore.audit: ugi=kentyao	ip=unknown-ip-addr	cmd=get_database: default
2020-11-16 23:50:52.972 INFO metastore.HiveMetaStore: 2: get_tables: db=default pat=*
2020-11-16 23:50:52.972 INFO HiveMetaStore.audit: ugi=kentyao	ip=unknown-ip-addr	cmd=get_tables: db=default pat=*
2020-11-16 23:50:52.986 INFO operation.ExecuteStatement: Processing kentyao's query[ff902582-ba29-433b-b70a-c25ead1353a8]: RUNNING_STATE -> FINISHED_STATE, statement: show tables, time taken: 0.03 seconds
+-----------+------------+--------------+
| database  | tableName  | isTemporary  |
+-----------+------------+--------------+
+-----------+------------+--------------+
No rows selected (0.04 seconds)
```
Using this mode for experimental purposes only.

In a real production environment, we always have a communal standalone metadata store,
to manage the metadata of persistent relational entities, e.g. databases, tables, columns, partitions, for fast access.
usually, Hive metastore as the defacto.

## Related Configurations

These are the basic needs for a Hive metastore client to communicate with the remote Hive Metastore server.

Use remote metastore database or server mode depends on the server-side configuration.

### Remote Metastore Database

Name | Value | Meaning
--- | --- | ---
javax.jdo.option.ConnectionURL | jdbc:mysql://&lt;hostname&gt;/&lt;databaseName&gt;?<br>createDatabaseIfNotExist=true | metadata is stored in a MySQL server
javax.jdo.option.ConnectionDriverName | com.mysql.jdbc.Driver | MySQL JDBC driver class
javax.jdo.option.ConnectionUserName | &lt;username&gt; | user name for connecting to MySQL server
javax.jdo.option.ConnectionPassword | &lt;password&gt; | password for connecting to MySQL server

### Remote Metastore Server

Name | Value | Meaning
--- | --- | ---
hive.metastore.uris | thrift://&lt;host&gt;:&lt;port&gt;,thrift://&lt;host1&gt;:&lt;port1&gt; | <div style='width: 200pt;word-wrap: break-word;white-space: normal'>host and port for the Thrift metastore server.</div>

## Activate Configurations

### Via kyuubi-defaults.conf

In `$KYUUBI_HOME/conf/kyuubi-defaults.conf`, all _**Hive primitive configurations**_, e.g. `hive.metastore.uris`,
and the **_Spark derivatives_**, which are prefixed with `spark.hive.` or `spark.hadoop.`, e.g `spark.hive.metastore.uris` or `spark.hadoop.hive.metastore.uris`,
will be loaded as Hive primitives by the Hive client inside the Spark application.

Kyuubi will take these configurations as system wide defaults for all applications it launches.

### Via hive-site.xml

Place your copy of `hive-site.xml` into `$$SPARK_HOME/conf`,
every single Spark application will automatically load this config file to its classpath.

This version of configuration has lower priority than those in `$KYUUBI_HOME/conf/kyuubi-defaults.conf`.

### Via JDBC Connection URL

We can pass _**Hive primitives**_ or **_Spark derivatives_** directly in the JDBC connection URL, e.g.

```
jdbc:hive2://localhost:10009/;#hive.metastore.uris=thrift://localhost:9083
```

This will override the defaults in `$$SPARK_HOME/conf/hive-site.xml` and `$KYUUBI_HOME/conf/kyuubi-defaults.conf` for each _**user account**_

With this feature, end users are possible to visit different Hive metastore server instance.
Similarly, this works for other services like HDFS, YARN too.

**Limitation:** As most Hive configurations are final and unmodifiable in Spark at runtime,
this only takes effect during instantiating the Spark applications and will be ignored when reusing an existing application.
So, keep this in our mind.

**!!!THIS WORKS ONLY ONCE!!!**

**!!!THIS WORKS ONLY ONCE!!!**

**!!!THIS WORKS ONLY ONCE!!!**

### Via SET syntax

Most Hive configurations are final and unmodifiable in Spark at runtime, so keep this in our mind.

**!!!THIS WON'T WORK!!!**

**!!!THIS WON'T WORK!!!**

**!!!THIS WON'T WORK!!!**

## Version Compatibility

If backward compatibility is guaranteed by Hive versioning,
we can always use a lower version Hive metastore client to communicate with the higher version Hive metastore server.

For example, Spark 3.0 was released with a builtin Hive client (2.3.7), so, ideally, the version of server should &gt;= 2.3.x.

If you do have a legacy Hive metastore server that cannot be easily upgraded, and you may face the issue by default like this,

```java
Caused by: org.apache.thrift.TApplicationException: Invalid method name: 'get_table_req'
	at org.apache.thrift.TServiceClient.receiveBase(TServiceClient.java:79)
	at org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Client.recv_get_table_req(ThriftHiveMetastore.java:1567)
	at org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Client.get_table_req(ThriftHiveMetastore.java:1554)
	at org.apache.hadoop.hive.metastore.HiveMetaStoreClient.getTable(HiveMetaStoreClient.java:1350)
	at org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient.getTable(SessionHiveMetaStoreClient.java:127)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.apache.hadoop.hive.metastore.RetryingMetaStoreClient.invoke(RetryingMetaStoreClient.java:173)
	at com.sun.proxy.$Proxy37.getTable(Unknown Source)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.apache.hadoop.hive.metastore.HiveMetaStoreClient$SynchronizedHandler.invoke(HiveMetaStoreClient.java:2336)
	at com.sun.proxy.$Proxy37.getTable(Unknown Source)
	at org.apache.hadoop.hive.ql.metadata.Hive.getTable(Hive.java:1274)
	... 93 more
```

To prevent this problem, we can use Spark's [Interacting with Different Versions of Hive Metastore](http://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html#interacting-with-different-versions-of-hive-metastore)

## Further Readings

- Hive Wiki
  - [Hive Metastore Administration](https://cwiki.apache.org/confluence/display/Hive/AdminManual+Metastore+Administration)
- Spark Online Documentation
  - [Custom Hadoop/Hive Configuration](http://spark.apache.org/docs/latest/configuration.html#custom-hadoophive-configuration)
  - [Hive Tables](http://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html)
