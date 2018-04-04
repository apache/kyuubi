# Configuration Guide 

[TOC]

Kyuubi provides several kinds of properites to configure the system:

**Kyuubi properties:** control most Kyuui server's own behaviors. Most of them determined on server starting. They can be treat like normal Spark properties by setting them in `spark-defaults.conf` file or via `--conf` parameter in server starting scripts.     

**Spark properties:** become session level options, which are used to generate a SparkContext instances and passed to Kyuubi Server by JDBC/ODBC connection strings. Setting them in `$SPARKHOME/conf/spark-defaults.conf` supplies with default values for each session.     

**Hive properties:**             
1. Hive client options which are used for SparkSessin to talk to Hive MetaStore Server could be configured in a `hive-site.xml`  and placed it in `$SPARKHOME/conf` directory, or treating them as Spark properties with `spark.hadoop.` prefix.           
2. Kyuubi Frontend Service options is a small part of HiveServer2.     

**Hadoop properties:** specifying `HADOOP_CONF_DIR` or `YARN_CONF_DIR` to the directory contains hadoop configuration files.

**Logging** can be configured through `$SPARKHOME/conf/log4j.properties`.

## Kyuubi Configurations

Kyuubi properties control most Kyuui server's own behaviors. Most of them determined on server starting. They can be treat like normal Spark properties by setting them in `spark-defaults.conf` file or via `--conf` parameter in server starting scripts.     

For instance, start Kyuubi with HA enabled.
```bash
$ bin/start-kyuubi.sh \ 
    --master yarn \
    --deploy-mode client \
    --driver-memory 10g \
    --conf spark.kyuubi.ha.enabled=true \
    --conf spark.kyuubi.ha.zk.quorum=zk1.server.url,zk2.server.url
```

#### High Availability

Name|Default|Description
---|---|---
spark.kyuubi.ha.enabled|false|Whether KyuubiServer supports dynamic service discovery for its clients. To support this, each instance of KyuubiServer currently uses ZooKeeper to register itself, when it is brought up. JDBC/ODBC clients should use the ZooKeeper ensemble: spark.kyuubi.ha.zk.quorum in their connection string.
spark.kyuubi.ha.zk.quorum|none|Comma separated list of ZooKeeper servers to talk to, when KyuubiServer supports service discovery via Zookeeper.
spark.kyuubi.ha.zk.namespace|kyuubiserver|The parent node in ZooKeeper used by KyuubiServer when supporting dynamic service discovery.
spark.kyuubi.ha.zk.client.port|2181|The port of ZooKeeper servers to talk to. If the list of Zookeeper servers specified in spark.kyuubi.zookeeper.quorum does not contain port numbers, this value is used.
spark.kyuubi.ha.zk.session.timeout|1,200,000|ZooKeeper client's session timeout (in milliseconds). The client is disconnected, and as a result, all locks released, if a heartbeat is not sent in the timeout.
spark.kyuubi.ha.zk.connection.basesleeptime|1,000|Initial amount of time (in milliseconds) to wait between retries when connecting to the ZooKeeper server when using ExponentialBackoffRetry policy.
spark.kyuubi.ha.zk.connection.max.retries|3|Max retry times for connecting to the zk server

#### Operation Log

Name|Default|Description
---|---|---
spark.kyuubi.logging.operation.enabled|true|When true, KyuubiServer will save operation logs and make them available for clients
spark.kyuubi.logging.operation.log.dir|`SPARK_LOG_DIR` -> `SPARK_HOME`/operation_logs -> `java.io.tmpdir`/operation_logs|Top level directory where operation logs are stored if logging functionality is enabled

#### Frontend Service options

Name|Default|Description
---|---|---
spark.kyuubi.frontend.bind.host | localhost | Bind host on which to run the Kyuubi Frontend service.
spark.kyuubi.frontend.bind.port| 10009 | Port number of Kyuubi Frontend service. set 0 will get a random available one
spark.kyuubi.frontend.min.worker.threads| 50 | Minimum number of Thrift worker threads.
spark.kyuubi.frontend.max.worker.threads| 500 | Maximum number of Thrift worker threads
spark.kyuubi.frontend.worker.keepalive.time | 60s| Keepalive time (in seconds) for an idle worker thread. When the number of workers exceeds min workers, excessive threads are killed after this time interval.
spark.kyuubi.authentication | NONE | Client authentication types. NONE: no authentication check  KERBEROS: Kerberos/GSSAPI authentication.
spark.kyuubi.frontend.allow.user.substitution | true | Allow alternate user to be specified as part of Kyuubi open connection request.
spark.kyuubi.frontend.enable.doAs | true | Set true to have Kyuubi execute SQL operations as the user making the calls to it.
spark.kyuubi.frontend.max.message.size | 104857600 | Maximum message size in bytes a Kyuubi server will accept.

#### Background Execution Thread Pool

Name|Default|Description
---|---|---
spark.kyuubi.async.exec.threads|100|Number of threads in the async thread pool for KyuubiServer.
spark.kyuubi.async.exec.wait.queue.size|100|Size of the wait queue for async thread pool in KyuubiServer. After hitting this limit, the async thread pool will reject new requests.
spark.kyuubi.async.exec.keep.alive.time|10,000|Time (in milliseconds) that an idle KyuubiServer async thread (from the thread pool) will wait for a new task to arrive before terminating.
spark.kyuubi.async.exec.shutdown.timeout|10,000|How long KyuubiServer shutdown will wait for async threads to terminate.

#### Session Idle Check

Name|Default|Description
---|---|---
spark.kyuubi.frontend.session.check.interval|6h|The check interval for frontend session/operation timeout, which can be disabled by setting to zero or negative value.
spark.kyuubi.frontend.session.timeout|8h|The check interval for session/operation timeout, which can be disabled by setting  to zero or negative value.
spark.kyuubi.frontend.session.check.operation| true |Session will be considered to be idle only if there is no activity, and there is no pending operation. This setting takes effect only if session idle timeout `spark.kyuubi.frontend.session.timeout` and checking `spark.kyuubi.frontend.session.check.interval` are enabled.
spark.kyuubi.backend.session.check.interval|20min|The check interval for backend session a.k.a SparkSession timeout.

#### On Spark Session Init

Name|Default|Description
---|---|---
spark.kyuubi.backend.session.wait.other.times | 60 | How many times to check when another session with the same user is initializing SparkContext. Total Time will be times by `spark.kyuubi.backend.session.wait.other.interval`.
spark.kyuubi.backend.session.wait.other.interval|1s|The interval for checking whether other thread with the same user has completed SparkContext instantiation.
spark.kyuubi.backend.session.init.timeout|60s|How long we suggest the server to give up instantiating SparkContext.

---

## Spark Configurations

Spark properties become session level options, which are used to generate a SparkContext instances and passed to Kyuubi Server by JDBC/ODBC connection strings. Setting them in `$SPARKHOME/conf/spark-defaults.conf` supplies with default values for each session.     

Name|Default|Description
---|---|---
spark.driver.memory| 1g | Amount of memory to use for the Kyuubi Server instance. Set this through the --driver-memory command line option or in your default properties file.
spark.driver.extraJavaOptions| (none) | A string of extra JVM options to pass to the Kyuubi Server instance. For instance, GC settings or other logging. Set this through the --driver-java-options command line option or in your default properties file.

Spark properties for [Driver](http://spark.apache.org/docs/latest/configuration.html#runtime-environment) like those above controls Kyuubi Server's own behaviors, while other properies could be set in JDBC/ODBC connection strings.

Please refer to the [Configuration Guide](http://spark.apache.org/docs/latest/configuration.html) in the online documentation for an overview on how to configure Spark.

## Hive Configurations

#### Hive client options
These configurations are used for SparkSessin to talk to Hive MetaStore Server could be configured in a `hive-site.xml`  and placed it in `$SPARKHOME/conf` directory, or treating them as Spark properties with `spark.hadoop.` prefix.

## Hadoop Configurations
Please refer to the [Apache Hadoop](http://hadoop.apache.org)'s online documentation for an overview on how to configure Hadoop.
