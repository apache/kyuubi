# Configurations

## Kyuubi Configurations
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

## Hive Configurations

Name|Default|Description
---|---|---
hive.server2.logging.operation.enabled | true | When true, Kyuubi Server will save operation logs and make them available for clients

## [Spark Configurations]()


## Hadoop Configurations

