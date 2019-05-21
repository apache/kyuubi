# Kyuubi Configuration Guide 

Kyuubi provides several kinds of properties to configure the system:

**Kyuubi properties:** control most Kyuubi server's own behaviors. Most of them determined on server starting. They can be treat like normal Spark properties by setting them in `spark-defaults.conf` file or via `--conf` parameter in server starting scripts.

**Spark properties:** become session level options, which are used to generate a SparkContext instances and passed to Kyuubi Server by JDBC/ODBC connection strings. Setting them in `$SPARK_HOME/conf/spark-defaults.conf` supplies with default values for each session.

**Hive properties:** are used for SparkSession to talk to the Hive MetaStore Server could be configured in a `hive-site.xml`  and placed it in `$SPARK_HOME/conf` directory, or treating them as Spark properties with `spark.hadoop.` prefix.

**Hadoop properties:** specifying `HADOOP_CONF_DIR` or `YARN_CONF_DIR` to the directory contains hadoop configuration files.

**Logging** can be configured through `$SPARK_HOME/conf/log4j.properties`.

## Kyuubi Configurations

Kyuubi properties control most Kyuubi server's own behaviors. Most of them determined on server starting. They can be treat like normal Spark properties by setting them in `spark-defaults.conf` file or via `--conf` parameter in server starting scripts.     

For instance, start Kyuubi with HA (load balance) enabled.
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
spark.kyuubi.<br />ha.enabled|false|Whether KyuubiServer supports dynamic service discovery for its clients. To support this, each instance of KyuubiServer currently uses ZooKeeper to register itself, when it is brought up. JDBC/ODBC clients should use the ZooKeeper ensemble: spark.kyuubi.ha.zk.quorum in their connection string.
spark.kyuubi.<br />ha.mode|load-balance|High availability mode, one is load-balance which is used by default, another is failover as master-slave mode.
spark.kyuubi.<br />ha.zk.quorum|none|Comma separated list of ZooKeeper servers to talk to, when KyuubiServer supports service discovery via Zookeeper.
spark.kyuubi.<br />ha.zk.namespace|kyuubiserver|The parent node in ZooKeeper used by KyuubiServer when supporting dynamic service discovery.
spark.kyuubi.<br />ha.zk.client.port|2181|The port of ZooKeeper servers to talk to. If the list of Zookeeper servers specified in spark.kyuubi.zookeeper.quorum does not contain port numbers, this value is used.
spark.kyuubi.<br />ha.zk.session.timeout|1,200,000|ZooKeeper client's session timeout (in milliseconds). The client is disconnected, and as a result, all locks released, if a heartbeat is not sent in the timeout.
spark.kyuubi.<br />ha.zk.connection.basesleeptime|1,000|Initial amount of time (in milliseconds) to wait between retries when connecting to the ZooKeeper server when using ExponentialBackoffRetry policy.
spark.kyuubi.<br />ha.zk.connection.max.retries|3|Max retry times for connecting to the zk server

#### Operation Log

Name|Default|Description
---|---|---
spark.kyuubi.<br />logging.operation.enabled|true|When true, Kyuubi Server will save operation logs and make them available for clients
spark.kyuubi.<br />logging.operation.log.dir|KYUUBI_LOG_DIR/<br />operation_logs|Top level directory where operation logs are stored if logging functionality is enabled

#### Frontend Service options

Name|Default|Description
---|---|---
spark.kyuubi.<br />frontend.bind.host | localhost | Bind host on which to run the Kyuubi Frontend service.
spark.kyuubi.<br />frontend.bind.port| 10009 | Port number of Kyuubi Frontend service. set 0 will get a random available one
spark.kyuubi.<br />frontend.min.worker.threads| 50 | Minimum number of Thrift worker threads.
spark.kyuubi.<br />frontend.max.worker.threads| 500 | Maximum number of Thrift worker threads
spark.kyuubi.<br />frontend.worker.keepalive.time | 60s| Keepalive time (in seconds) for an idle worker thread. When the number of workers exceeds min workers, excessive threads are killed after this time interval.
spark.kyuubi.<br />authentication | NONE | Client authentication types. NONE: no authentication check; NOSASL: no authentication check  KERBEROS: Kerberos/GSSAPI authentication.
spark.kyuubi.<br />frontend.allow.user.substitution | true | Allow alternate user to be specified as part of Kyuubi open connection request.
spark.kyuubi.<br />frontend.enable.doAs | true | Set true to have Kyuubi execute SQL operations as the user making the calls to it.
spark.kyuubi.<br />frontend.max.message.size | 104857600 | Maximum message size in bytes a Kyuubi server will accept.

#### Background Execution Thread Pool

Name|Default|Description
---|---|---
spark.kyuubi.<br />async.exec.threads|100|Number of threads in the async thread pool for KyuubiServer.
spark.kyuubi.<br />async.exec.wait.queue.size|100|Size of the wait queue for async thread pool in KyuubiServer. After hitting this limit, the async thread pool will reject new requests.
spark.kyuubi.<br />async.exec.keep.alive.time|10,000|Time (in milliseconds) that an idle KyuubiServer async thread (from the thread pool) will wait for a new task to arrive before terminating.
spark.kyuubi.<br />async.exec.shutdown.timeout|10,000|How long KyuubiServer shutdown will wait for async threads to terminate.

#### Kyuubi Session

Name|Default|Description
---|---|---
spark.kyuubi.<br />frontend.session.check.interval|6h|The check interval for frontend session/operation timeout, which can be disabled by setting to zero or negative value.
spark.kyuubi.<br />frontend.session.timeout|8h|The check interval for session/operation timeout, which can be disabled by setting  to zero or negative value.
spark.kyuubi.<br />frontend.session.check.operation| true |Session will be considered to be idle only if there is no activity, and there is no pending operation. This setting takes effect only if session idle timeout `spark.kyuubi.frontend.session.timeout` and checking `spark.kyuubi.frontend.session.check.interval` are enabled.

#### Spark Session

Name|Default|Description
---|---|---
spark.kyuubi.<br />backend.session.wait.other.times | 60 | How many times to check when another session with the same user is initializing SparkContext. Total Time will be times by `spark.kyuubi.backend.session.wait.other.interval`.
spark.kyuubi.<br />backend.session.wait.other.interval|1s|The interval for checking whether other thread with the same user has completed SparkContext instantiation.
spark.kyuubi.<br />backend.session.init.timeout|60s|How long we suggest the server to give up instantiating SparkContext.
spark.kyuubi.<br />backend.session.check.interval|5min|The check interval for backend session a.k.a SparkSession timeout.
spark.kyuubi.<br />backend.session.idle.timeout|30min|How long the SparkSession instance will be cached after user logout. Using cached SparkSession can significantly cut the startup time for SparkContext, which makes sense for queries that are short lived. The timeout is calculated from when all sessions of the user are disconnected
spark.kyuubi.<br />backend.session.max.cache.time|5d|Max cache time for a SparkSession instance when its original copy has been created. When `spark.kyuubi.backend.session.idle.timeout` never is reached for user may continuously run queries, we need this configuration to stop the cached SparkSession which may end up with token expiry issue in kerberized clusters. When in the interval of [t, t * 1.25], we will try to stop the SparkSession gracefully util no connections. But once it fails stop in that region, we will force to stop it
spark.kyuubi.<br />backend.session.local.dir|KYUUBI_HOME/<br />local|Default value to set `spark.local.dir`. For YARN mode, this only affect the Kyuubi server side settings according to the rule of Spark treating `spark.local.dir`.

#### Operation

Name|Default|Description
---|---|---
spark.kyuubi.<br />operation.idle.timeout|6h|Operation will be closed when it's not accessed for this duration of time.
spark.kyuubi.<br />operation.incremental.collect|false|Whether to use incremental result collection from Spark executor side to Kyuubi server side.
spark.kyuubi.<br />operation.result.limit|-1|In non-incremental result collection mode, set this to a positive value to limit the size of result collected to driver side.

---

## Spark Configurations

All properties of Spark can be set as server level ones. Some of them only work for Kyuubi server itself and become immutable, such as `spark.driver.memory` specifying the heap memory of server. Session level Spark properties take Server lever ones as default values and can be changed with session connection strings. And obviously, all sql properties of Spark can be set via `set` statement at runtime, such as `set spark.sql.autoBroadcastJoinThreshold=-1`

#### Session Level

Spark properties which becomes session level options, which are used to generate a `SparkContext` instances and passed to Kyuubi Server by JDBC/ODBC connection strings. Setting them in `$SPARK_HOME/conf/spark-defaults.conf` supplies with default values for each session.     

#### Server Level

Name|Default|Description
---|---|---
spark.driver.memory| 1g | Amount of memory to use for the Kyuubi Server instance. Set this through the --driver-memory command line option or in your default properties file.
spark.driver.extraJavaOptions| (none) | A string of extra JVM options to pass to the Kyuubi Server instance. For instance, GC settings or other logging. Set this through the --driver-java-options command line option or in your default properties file.

Spark uses netty as RPC between driver and executor, Kyuubi Server may need much bigger directory memory size.

```properties
spark.driver.extraJavaOptions -XX:+PrintFlagsFinal -XX:+UnlockDiagnosticVMOptions -XX:ParGCCardsPerStrideChunk=4096 -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSConcurrentMTEnabled -XX:CMSInitiatingOccupancyFraction=70 -XX:+UseCMSInitiatingOccupancyOnly -XX:+CMSClassUnloadingEnabled -XX:+CMSParallelRemarkEnabled -XX:+UseCondCardMark -XX:PermSize=1024m -XX:MaxPermSize=1024m -XX:MaxDirectMemorySize=8192m  -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./logs -XX:OnOutOfMemoryError="kill -9 %p" -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -Xloggc:./logs/kyuubi-server-gc-%t.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=50 -XX:GCLogFileSize=5M  -XX:NewRatio=3 -Dio.netty.noPreferDirect=true -Dio.netty.recycler.maxCapacity=0
```

Spark properties for [Driver](http://spark.apache.org/docs/latest/configuration.html#runtime-environment) like those above controls Kyuubi Server's own behaviors, while other properties could be set in JDBC/ODBC connection strings.

Please refer to the [Configuration Guide](http://spark.apache.org/docs/latest/configuration.html) in the online documentation for an overview on how to configure Spark.

## Hive Configurations

#### Hive client options

These configurations are used for SparkSession to talk to Hive MetaStore Server could be configured in a `hive-site.xml`  and placed it in `$SPARK_HOME/conf` directory, or treating them as Spark properties with `spark.hadoop.` prefix.

## Hadoop Configurations

Please refer to the [Apache Hadoop](http://hadoop.apache.org)'s online documentation for an overview on how to configure Hadoop.

## Additional Documentations

[Building Kyuubi](https://yaooqinn.github.io/kyuubi/docs/building.html)  
[Kyuubi Deployment Guide](https://yaooqinn.github.io/kyuubi/docs/deploy.html)   
[Kyuubi Containerization Guide](https://yaooqinn.github.io/kyuubi/docs/containerization.html)   
[High Availability Guide](https://yaooqinn.github.io/kyuubi/docs/high_availability_guide.html)  
[Authentication/Security Guide](https://yaooqinn.github.io/kyuubi/docs/authentication.html)  
[Kyuubi ACL Management Guide](https://yaooqinn.github.io/kyuubi/docs/authorization.html)  
[Kyuubi Architecture](https://yaooqinn.github.io/kyuubi/docs/architecture.html)  
[Home Page](https://yaooqinn.github.io/kyuubi/)
