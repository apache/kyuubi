package org.apache.kyuubi.common;

import org.apache.commons.lang3.reflect.FieldUtils;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum KyuubiConfigs {

    @Version(Version.V1)
    KYUUBI_SERVER_ID("spark.kyuubi.instance.id", null, "kyuubi server instance id"),

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //                                WEB Configuration                                            //
    /////////////////////////////////////////////////////////////////////////////////////////////////
    @Version(Version.V1)
    KYUUBI_AUDIT_ENABLED("spark.kyuubi.audit.enabled", false, "kyuubi audit enabled conf"),

    @Version(Version.V1)
    KYUUBI_AUDIT_ADMIN("spark.kyuubi.audit.admin", "hive", "web admin who can kill spark job"),

    @Version(Version.V1)
    KYUUBI_AUDIT_HTTP_PORT("spark.kyuubi.audit.http.port", 8100, "audit http port"),

    @Version(Version.V20210305)
    KYUUBI_AUDIT_HTTP_HOST("spark.kyuubi.audit.http.host", "0.0.0.0", "audit http host"),

    @Version(Version.V20210305)
    KYUUBI_AUDIT_AUTH_ENABLED("spark.kyuubi.audit.authorization", false, "audit auth enabled"),


    @Version(Version.V1)
    KYUUBI_AUDIT_CLEAN_INTERVAL_DAYS("spark.kyuubi.audit.clean.interval_days", 30, "clean interval days"),

    @Version(Version.V1)
    KYUUBI_AUDIT_DB_DRIVER("spark.kyuubi.audit.jdbc.driver", "com.mysql.cj.jdbc.Driver", "KYUUBI_AUDIT_DB_DRIVER"),

    @Version(Version.V1)
    KYUUBI_AUDIT_DB_URL("spark.kyuubi.audit.jdbc.url", null, "mysql jdbc url"),

    @Version(Version.V1)
    KYUUBI_AUDIT_DB_USER("spark.kyuubi.audit.jdbc.user", null, "mysql jdbc user"),

    @Version(Version.V1)
    KYUUBI_AUDIT_DB_PASSWORD("spark.kyuubi.audit.jdbc.passwd", null, "mysql jdbc password"),

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //                                High Availability by ZooKeeper                               //
    /////////////////////////////////////////////////////////////////////////////////////////////////
    @Version(Version.V1)
    @Unused(version = Version.V20210305)
    HA_ENABLED("spark.kyuubi.ha.enabled", false,
            "Whether KyuubiServer supports dynamic service discovery for its clients." +
                    " To support this, each instance of KyuubiServer currently uses ZooKeeper to" +
                    " register itself, when it is brought up. JDBC/ODBC clients should use the " +
                    "ZooKeeper ensemble: spark.kyuubi.ha.zk.quorum in their connection string."),

    @Version(Version.V1)
    @Unused(version = Version.V20210305)
    HA_MODE("spark.kyuubi.ha.mode", "load-balance",
            "Hive availability mode, one is load-balance which is used by default, anthoner is " +
                    "failover as master-slave mode"),

    @Version(Version.V1)
    @Unused(version = Version.V20210305)
    HA_ZOOKEEPER_QUORUM("spark.kyuubi.ha.zk.quorum", null,
            "Comma separated list of zookeeper servers to talk to, when KyuubiServer support" +
                    " service discover via zookeeper"),

    @Version(Version.V1)
    @Unused(version = Version.V20210305)
    HA_ZOOKEEPER_NAMESPACE("spark.kyuubi.ha.zk.namespace", "Kyuubiserver",
            "The parent node in Zookeeper used by KyuubiServer when supporting dynamic service" +
                    "discovery."),

    @Version(Version.V1)
    @Unused(version = Version.V20210305)
    HA_ZOOKEEPER_CLIENT_PORT("spark.kyuubi.ha.zk.client.port", 2181,
            "The port of ZooKeeper servers to talk to. If the list of Zookeeper servers specified" +
                    " in spark.kyuubi.zookeeper.quorum does not contain port numbers, this value is used."),

    @Version(Version.V1)
    @Unused(version = Version.V20210305)
    HA_ZOOKEEPER_SESSION_TIMEOUT("spark.kyuubi.ha.zk.session.timeout", "20m",
            "ZooKeeper client's session timeout (in milliseconds). The client is disconnected, and" +
                    " as a result, all locks released, if a heartbeat is not sent in the timeout."),

    @Version(Version.V1)
    @Unused(version = Version.V20210305)
    HA_ZOOKEEPER_CONNECTION_BASESLEEPTIME("spark.kyuubi.ha.zk.connection.basesleeptime", "1s",
            "Initial amount of time (in milliseconds) to wait between retries when connecting to" +
                    " the ZooKeeper server when using ExponentialBackoffRetry policy."),

    @Version(Version.V1)
    @Unused(version = Version.V20210305)
    HA_ZOOKEEPER_CONNECTION_MAX_RETRIES("spark.kyuubi.ha.zk.connection.max.retries", 3, "Max retry times for connecting to the zk server"),

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //                                      Operation Log                                          //
    /////////////////////////////////////////////////////////////////////////////////////////////////
    @Version(Version.V1)
    LOGGING_OPERATION_ENABLED("spark.kyuubi.logging.operation.enabled", true,
            "When true, KyuubiServer will save operation logs and make them available for clients"),

    @Version(Version.V1)
    LOGGING_OPERATION_LOG_DIR("spark.kyuubi.logging.operation.log.dir",
            System.getenv().getOrDefault("KYUUBI_LOG_DIR", System.getProperty("java.io.tmpdir")) + File.separator + "operation_logs",
            "Top level directory where operation logs are stored if logging functionality is" +
                    " enabled"),

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //                              Background Execution Thread Pool                               //
    /////////////////////////////////////////////////////////////////////////////////////////////////
    @Version(Version.V1)
    ASYNC_EXEC_THREADS("spark.kyuubi.async.exec.threads", 4,
            "Number of threads in the async thread pool for KyuubiServer"),
    @Version(Version.V20210305)
    ASYNC_EXEC_THREADS_MAX("spark.kyuubi.async.exec.threads.max", 1000,
            "Number of max threads in the async thread pool for KyuubiServer"),

    @Version(Version.V1)
    ASYNC_EXEC_WAIT_QUEUE_SIZE("spark.kyuubi.async.exec.wait.queue.size", 1000,
            "Size of the wait queue for async thread pool in KyuubiServer. After hitting this" +
                    " limit, the async thread pool will reject new requests."),

    @Version(Version.V1)
    EXEC_KEEPALIVE_TIME("spark.kyuubi.async.exec.keep.alive.time", "10s",
            "Time (in milliseconds) that an idle KyuubiServer async thread (from the thread pool)" +
                    " will wait for a new task to arrive before terminating"),

    @Version(Version.V1)
    @Unused(version = Version.V20210305)
    ASYNC_EXEC_SHUTDOWN_TIMEOUT("spark.kyuubi.async.exec.shutdown.timeout", "10s",
            "How long KyuubiServer shutdown will wait for async threads to terminate."),

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //                                       kyuubi Session                                        //
    /////////////////////////////////////////////////////////////////////////////////////////////////
    @Version(Version.V1)
    FRONTEND_SESSION_CHECK_INTERVAL("spark.kyuubi.frontend.session.check.interval", "6h",
            "The check interval for frontend session/operation timeout, which can be disabled by" +
                    " setting to zero or negative value."),

    @Version(Version.V1)
    FRONTEND_IDLE_SESSION_TIMEOUT("spark.kyuubi.frontend.session.timeout", "8h",
            "The check interval for session/operation timeout, which can be disabled by setting" +
                    " to zero or negative value."),

    @Version(Version.V1)
    @Unused(version = Version.V20210305)
    FRONTEND_IDLE_SESSION_CHECK_OPERATION("spark.kyuubi.frontend.session.check.operation", true,
            "Session will be considered to be idle only if there is no activity, and there is no" +
                    " pending operation. This setting takes effect only if session idle timeout" +
                    " (spark.kyuubi.frontend.session.timeout) and checking" +
                    " (spark.kyuubi.frontend.session.check.interval) are enabled."),

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //                              Frontend Service Configuration                                 //
    /////////////////////////////////////////////////////////////////////////////////////////////////
    @Version(Version.V1)
    FRONTEND_BIND_HOST("spark.kyuubi.frontend.bind.host", "0.0.0.0",
            "Bind host on which to run the kyuubi Server."),

    @Version(Version.V1)
    FRONTEND_BIND_PORT("spark.kyuubi.frontend.bind.port", 10009, "Bind port on which to run the kyuubi Server."),


    @Version(Version.V1)
    FRONTEND_WORKER_KEEPALIVE_TIME("spark.kyuubi.frontend.worker.keepalive.time", "60s",
            "Keep-alive time (in seconds) for an idle worker thread"),

    @Version(Version.V1)
    FRONTEND_MIN_WORKER_THREADS("spark.kyuubi.frontend.min.worker.threads", 1,
            "Minimum number of threads in the of frontend worker thread pool for KyuubiServer"),

    @Version(Version.V1)
    FRONTEND_MAX_WORKER_THREADS("spark.kyuubi.frontend.max.worker.threads", 500,
            "Maximum number of threads in the of frontend worker thread pool for KyuubiServer"),

    @Version(Version.V1)
    FRONTEND_ALLOW_USER_SUBSTITUTION("spark.kyuubi.frontend.allow.user.substitution", true,
            "Allow alternate user to be specified as part of open connection request."),

    @Version(Version.V1)
    FRONTEND_ENABLE_DOAS("spark.kyuubi.frontend.enable.doAs", true,
            "Setting this property to true enables executing operations as the user making the" +
                    " calls to it."),

    @Version(Version.V1)
    FRONTEND_MAX_MESSAGE_SIZE("spark.kyuubi.frontend.max.message.size", 104857600, "Maximum message size in bytes a kyuubi server will accept."),

    @Version(Version.V1)
    FRONTEND_LOGIN_TIMEOUT("spark.kyuubi.frontend.login.timeout", "20s", "Timeout for Thrift clients during login to kyuubi Server."),

    @Version(Version.V1)
    FRONTEND_LOGIN_BACKOFF_SLOT_LENGTH("spark.kyuubi.frontend.backoff.slot.length", "100s",
            "Time to back off during login to kyuubi Server."),

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //                                        SparkSession                                         //
    /////////////////////////////////////////////////////////////////////////////////////////////////
    @Version(Version.V1)
    BACKEND_SESSION_INIT_TIMEOUT("spark.kyuubi.backend.session.init.timeout", "60s",
            "How long we suggest the server to give up instantiating SparkContext"),

    @Version(Version.V1)
    BACKEND_SESSION_CHECK_INTERVAL("spark.kyuubi.backend.session.check.interval", "1m",
            "The check interval for backend session a.k.a SparkSession timeout"),

    @Version(Version.V1)
    BACKEND_SESSION_IDLE_TIMEOUT("spark.kyuubi.backend.session.idle.timeout", "30m",
            "How long the SparkSession instance will be cached after user logout. Using cached" +
                    " SparkSession can significantly cut the startup time for SparkContext, which makes" +
                    " sense for queries that are short lived. The timeout is calculated from when all" +
                    " sessions of the user are disconnected"),

    @Version(Version.V1)
    @Unused(version = Version.V20210305)
    BACKEND_SESSION_MAX_CACHE_TIME("spark.kyuubi.backend.session.max.cache.time", "5d",
            "Max cache time for a SparkSession instance when its original copy has been created." +
                    " When `spark.kyuubi.backend.session.idle.timeout` never is reached for user may" +
                    " continuously run queries, we need this configuration to stop the cached SparkSession" +
                    " which may end up with token expiry issue in kerberized clusters. When in the interval" +
                    " of [t, t * 1.25], we will try to stop the SparkSession gracefully util no connections." +
                    " But once it fails stop in that region, we will force to stop it"),

    @Version(Version.V1)
    BACKEND_SESSION_LOCAL_DIR("spark.kyuubi.backend.session.local.dir",
            System.getenv().getOrDefault("KYUUBI_HOME", System.getProperty("java.io.tmpdir")) + File.separator + "local",
            "Default value to set `spark.local.dir`, for YARN mode, this only affect the kyuubi" +
                    " server side settings according to the rule of Spark treating `spark.local.dir`"),

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //                                      Authentication                                         //
    /////////////////////////////////////////////////////////////////////////////////////////////////
    @Version(Version.V1)
    AUTHENTICATION_METHOD("spark.kyuubi.authentication", "NONE",
            "Client authentication types." +
                    " NONE: no authentication check." +
                    " KERBEROS: Kerberos/GSSAPI authentication." +
                    " LDAP: Lightweight Directory Access Protocol authentication."),

    @Version(Version.V1)
    SASL_QOP("spark.kyuubi.sasl.qop", "auth",
            "Sasl QOP enable higher levels of protection for kyuubi communication with clients." +
                    " auth - authentication only (default)" +
                    " auth-int - authentication plus integrity protection" +
                    " auth-conf - authentication plus integrity and confidentiality protectionThis is" +
                    " applicable only if kyuubi is configured to use Kerberos authentication."),

    @Version(Version.V1)
    AUTHENTICATION_LDAP_URL("spark.kyuubi.authentication.ldap.url", null, "SPACE character separated LDAP connection URL(s)."),

    @Version(Version.V1)
    AUTHENTICATION_LDAP_BASEDN("spark.kyuubi.authentication.ldap.baseDN", null, "LDAP base DN."),

    @Version(Version.V1)
    AUTHENTICATION_LDAP_DOMAIN("spark.kyuubi.authentication.ldap.Domain", null, "LDAP base DN."),

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //                                      Authorization                                          //
    /////////////////////////////////////////////////////////////////////////////////////////////////
    @Version(Version.V1)
    //TODO
            AUTHORIZATION_METHOD("spark.kyuubi.authorization.class", "org.apache.spark.sql.catalyst.optimizer.Authorizer",
            "A Rule[LogicalPlan] to support kyuubi with Authorization."),

    @Version(Version.V1)
    AUTHORIZATION_ENABLE("spark.kyuubi.authorization.enabled", false, "When true, kyuubi authorization is enabled."),

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //                                         Operation                                           //
    /////////////////////////////////////////////////////////////////////////////////////////////////
    @Version(Version.V1)
    OPERATION_IDLE_TIMEOUT("spark.kyuubi.operation.idle.timeout", "6h",
            "Operation will be closed when it's not accessed for this duration of time"),

    @Version(Version.V1)
    OPERATION_INCREMENTAL_COLLECT("spark.kyuubi.operation.incremental.collect", false,
            "Whether to use incremental result collection from Spark executor side to kyuubi" +
                    " server side"),

    @Version(Version.V1)
    OPERATION_INCREMENTAL_RDD_PARTITIONS_LIMIT("spark.kyuubi.operation.incremental.rdd.partitions.limit", 50,
            "In incremental result collection, when the partition number of the rdd underlying the" +
                    " query is great than this setting, kyuubi will try to coalesce first before calling" +
                    " toLocalIterator"),

    @Version(Version.V1)
    OPERATION_INCREMENTAL_PARTITION_ROWS("spark.kyuubi.operation.incremental.partition.rows", 20000,
            "In incremental result collection, Spark will run job not task on a single partition," +
                    " which sequentially get results one partition by one to the driver. we use this" +
                    " configuration and the total size of the query output to calculate the partition number" +
                    " to coalesce to. Use Math.min(`total size of the query output` / `spark.kyuubi.operation" +
                    ".incremental.collect.partition.rows`, `df.rdd.partition.size`) to limit the total number" +
                    " of jobs. In case of OutOfMemoryError happens frequently in executor side which lead to" +
                    " job failures, we suggest to decrease this number. Otherwise for performance reasons," +
                    " increase this setting to reduce the size of sequential Spark jobs"),

    @Version(Version.V1)
    OPERATION_RESULT_LIMIT("spark.kyuubi.operation.result.limit", -1,
            "In non-incremental result collection mode, set this to a positive value to limit the" +
                    " size of result collected to driver side."),

    @Version(Version.V1)
    OPERATION_DOWNLOADED_RESOURCES_DIR("spark.kyuubi.operation.downloaded.resources.dir",
            System.getenv().getOrDefault("KYUUBI_HOME", System.getProperty("java.io.tmpdir")) + File.separator + "resources",
            "Temporary local directory for added resources in the remote file system."),

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //                                   Containerization                                          //
    /////////////////////////////////////////////////////////////////////////////////////////////////
    @Version(Version.V1)
    @Unused(version = Version.V20210305)
    YARN_CONTAINER_TIMEOUT("spark.kyuubi.yarn.container.timeout", "60s",
            "Timeout for client to wait kyuubi successfully initialising itself"),

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //                                        Metrics                                              //
    /////////////////////////////////////////////////////////////////////////////////////////////////
    @Version(Version.V1)
    METRICS_ENABLE("spark.kyuubi.metrics.enabled", false, "Whether to enable kyuubi metrics system"),

    @Version(Version.V1)
    METRICS_REPORTER("spark.kyuubi.metrics.reporter", "JSON",
            "Comma separated list of reporters for kyuubi metrics system, candidates:" +
                    " JMX,CONSOLE,JSON,HTTP,INFLUX,PROMETHEUS"),

    @Version(Version.V1)
    METRICS_REPORT_INTERVAL("spark.kyuubi.metrics.report.interval", "5s",
            "How often should report metrics to json/console"),

    @Version(Version.V1)
    METRICS_REPORT_HTTP_PORT("spark.kyuubi.metrics.reporter.http.port", 8099,
            "only used for metric reporter HTTP as service port"),

    @Version(Version.V1)
    @Unused(version = Version.V20210305)
    METRICS_REPORT_INFLUX_HOST("spark.kyuubi.metrics.reporter.influx.host", null,
            "only used for metric reporter INFLUX"),

    @Version(Version.V1)
    @Unused(version = Version.V20210305)
    METRICS_REPORT_INFLUX_PORT("spark.kyuubi.metrics.reporter.influx.port", 8086,
            "only used for metric reporter INFLUX"),

    @Version(Version.V1)
    @Unused(version = Version.V20210305)
    METRICS_REPORT_INFLUX_USER("spark.kyuubi.metrics.reporter.influx.user", null,
            "only used for metric reporter INFLUX"),

    @Version(Version.V1)
    @Unused(version = Version.V20210305)
    METRICS_REPORT_INFLUX_PWD("spark.kyuubi.metrics.reporter.influx.pwd", null,
            "only used for metric reporter INFLUX"),

    @Version(Version.V1)
    @Unused(version = Version.V20210305)
    METRICS_REPORT_INFLUX_DB("spark.kyuubi.metrics.reporter.influx.db", "Kyuubi_metrics",
            "only used for metric reporter INFLUX"),

    @Version(Version.V1)
    @Unused(version = Version.V20210305)
    METRICS_REPORT_PROMETHEUS_PUSHWAY("spark.kyuubi.metrics.reporter.prometheus.pushway", "127.0.0.1:9091",
            "only used for metric reporter PROMETHEUS"),

    @Version(Version.V1)
    @Unused(version = Version.V20210305)
    METRICS_REPORT_PROMETHEUS_APP_NAME("spark.kyuubi.metrics.reporter.prometheus.appname", "kyuubi-metrics",
            "only used for metric reporter PROMETHEUS"),

    @Version(Version.V1)
    METRICS_REPORT_LOCATION("spark.kyuubi.metrics.report.location",
            System.getenv().getOrDefault("KYUUBI_HOME", System.getProperty("java.io.tmpdir"))
                    + File.separator + "metrics" + File.separator + "report.json",
            "Where the json metrics file located"),


    /**
     * 单用户并发配置
     */
    @Version(Version.V1)
    USER_MULTIPLE_NUM("spark.kyuubi.user.multiple.num", 1, "单用户并发数量"),

    /**
     * 小文件压缩配置
     */
    @Version(Version.V20210305)
    SPARK_COMPACK_ENABLED("spark.compact.enabled", false, "在Spark作业的结束时，是否启用压缩结果文件"),

    @Version(Version.V20210305)
    SPARK_KYUUBI_EXECUTOR_EXTRA_JARS("spark.kyuubi.executor.extraJars", "", "Spark Executor runtime jar from hdfs"),

    @Version(Version.V20210305)
    SPARK_HOODIE_READ_OPTIMISED ("spark.hoodie.read.optimized", "false", "hudi mor table default read type"),

    @Version(Version.V20210305)
    SPARK_BACKEND_PROXY_USER ("spark.kyuubi.backend.proxy.user", "", "")
    ;

    public static final Map<String, String> defaults;

    static {
        Map<String, String> keys = new HashMap<>();
        for (KyuubiConfigs value : KyuubiConfigs.values()) {
            if (value.defaultValue != null) {
                keys.put(value.key, String.valueOf(value.defaultValue));
            }
        }
        defaults = Collections.unmodifiableMap(keys);
    }

    private String key;
    private Object defaultValue;
    private String doc;
    private boolean immutable = false;

    KyuubiConfigs(String key, Object defaultValue, String doc, boolean immutable) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.doc = doc;
        this.immutable = immutable;
    }

    KyuubiConfigs(String key, Object defaultValue, String doc) {
        this(key, defaultValue, doc, false);
    }

    public String getKey() {
        return key;
    }

    public String getDoc() {
        return doc;
    }

    public String getDefaultValue() {
        return defaultValue == null ? null : String.valueOf(defaultValue);
    }

    public boolean isImmutable() {
        return immutable;
    }

    public boolean isDeprecated() {
        String name = this.name();
        Deprecated annotation = FieldUtils.getField(this.getClass(), name).getAnnotation(Deprecated.class);
        return annotation != null;
    }
}
