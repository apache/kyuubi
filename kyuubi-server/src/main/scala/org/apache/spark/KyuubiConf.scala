/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.io.File
import java.util.{HashMap => JMap}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}

/**
 * Kyuubi server level configuration which will be set when at the very beginning of server start.
 *
 */
object KyuubiConf {

  private val kyuubiConfEntries = new JMap[String, ConfigEntry[_]]()

  def register(entry: ConfigEntry[_]): Unit = {
    require(!kyuubiConfEntries.containsKey(entry.key),
      s"Duplicate SQLConfigEntry. ${entry.key} has been registered")
    kyuubiConfEntries.put(entry.key, entry)
  }

  private object KyuubiConfigBuilder {
    def apply(key: String): ConfigBuilder = ConfigBuilder(key).onCreate(register)
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //                                High Availability by ZooKeeper                               //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  val HA_ENABLED: ConfigEntry[Boolean] =
    KyuubiConfigBuilder("spark.kyuubi.ha.enabled")
      .doc("Whether KyuubiServer supports dynamic service discovery for its clients." +
        " To support this, each instance of KyuubiServer currently uses ZooKeeper to" +
        " register itself, when it is brought up. JDBC/ODBC clients should use the " +
        "ZooKeeper ensemble: spark.kyuubi.ha.zk.quorum in their connection string.")
    .booleanConf
    .createWithDefault(false)

  val HA_MODE: ConfigEntry[String] =
    KyuubiConfigBuilder("spark.kyuubi.ha.mode")
      .doc("High availability mode, one is load-balance which is used by default, another is " +
        "failover as master-slave mode")
      .stringConf
      .createWithDefault("load-balance")

  val HA_ZOOKEEPER_QUORUM: ConfigEntry[String] =
    KyuubiConfigBuilder("spark.kyuubi.ha.zk.quorum")
      .doc("Comma separated list of ZooKeeper servers to talk to, when KyuubiServer supports" +
      " service discovery via Zookeeper.")
      .stringConf
      .createWithDefault("")

  val HA_ZOOKEEPER_NAMESPACE: ConfigEntry[String] =
    KyuubiConfigBuilder("spark.kyuubi.ha.zk.namespace")
      .doc("The parent node in ZooKeeper used by KyuubiServer when supporting dynamic service" +
        " discovery.")
      .stringConf
      .createWithDefault("kyuubiserver")

  val HA_ZOOKEEPER_CLIENT_PORT: ConfigEntry[String] =
    KyuubiConfigBuilder("spark.kyuubi.ha.zk.client.port")
      .doc("The port of ZooKeeper servers to talk to. If the list of Zookeeper servers specified" +
        " in spark.kyuubi.zookeeper.quorum does not contain port numbers, this value is used.")
      .stringConf
      .createWithDefault("2181")

  val HA_ZOOKEEPER_SESSION_TIMEOUT: ConfigEntry[Long] =
    KyuubiConfigBuilder("spark.kyuubi.ha.zk.session.timeout")
      .doc("ZooKeeper client's session timeout (in milliseconds). The client is disconnected, and" +
      " as a result, all locks released, if a heartbeat is not sent in the timeout.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.MINUTES.toMillis(20L))

  val HA_ZOOKEEPER_CONNECTION_BASESLEEPTIME: ConfigEntry[Long] =
    KyuubiConfigBuilder("spark.kyuubi.ha.zk.connection.basesleeptime")
      .doc("Initial amount of time (in milliseconds) to wait between retries when connecting to" +
        " the ZooKeeper server when using ExponentialBackoffRetry policy.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.SECONDS.toMillis(1L))

  val HA_ZOOKEEPER_CONNECTION_MAX_RETRIES: ConfigEntry[Int] =
    KyuubiConfigBuilder("spark.kyuubi.ha.zk.connection.max.retries")
      .doc("Max retry times for connecting to the zk server")
      .intConf
      .createWithDefault(3)

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //                                      Operation Log                                          //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  val LOGGING_OPERATION_ENABLED: ConfigEntry[Boolean] =
    KyuubiConfigBuilder("spark.kyuubi.logging.operation.enabled")
      .doc("When true, KyuubiServer will save operation logs and make them available for clients")
      .booleanConf
      .createWithDefault(true)

  val LOGGING_OPERATION_LOG_DIR: ConfigEntry[String] =
    KyuubiConfigBuilder("spark.kyuubi.logging.operation.log.dir")
      .doc("Top level directory where operation logs are stored if logging functionality is" +
        " enabled")
      .stringConf
      .createWithDefault(
        s"${sys.env.getOrElse("KYUUBI_LOG_DIR", System.getProperty("java.io.tmpdir"))}"
          + File.separator + "operation_logs")

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //                              Background Execution Thread Pool                               //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  val ASYNC_EXEC_THREADS: ConfigEntry[Int] =
    KyuubiConfigBuilder("spark.kyuubi.async.exec.threads")
      .doc("Number of threads in the async thread pool for KyuubiServer")
      .intConf
      .createWithDefault(100)

  val ASYNC_EXEC_WAIT_QUEUE_SIZE: ConfigEntry[Int] =
    KyuubiConfigBuilder("spark.kyuubi.async.exec.wait.queue.size")
      .doc("Size of the wait queue for async thread pool in KyuubiServer. After hitting this" +
        " limit, the async thread pool will reject new requests.")
      .intConf
      .createWithDefault(100)

  val EXEC_KEEPALIVE_TIME: ConfigEntry[Long] =
    KyuubiConfigBuilder("spark.kyuubi.async.exec.keep.alive.time")
      .doc("Time (in milliseconds) that an idle KyuubiServer async thread (from the thread pool)" +
        " will wait for a new task to arrive before terminating")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.SECONDS.toMillis(10L))

  val ASYNC_EXEC_SHUTDOWN_TIMEOUT: ConfigEntry[Long] =
    KyuubiConfigBuilder("spark.kyuubi.async.exec.shutdown.timeout")
      .doc("How long KyuubiServer shutdown will wait for async threads to terminate.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.SECONDS.toMillis(10L))

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //                                       Kyuubi Session                                        //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  val FRONTEND_SESSION_CHECK_INTERVAL: ConfigEntry[Long] =
    KyuubiConfigBuilder("spark.kyuubi.frontend.session.check.interval")
      .doc("The check interval for frontend session/operation timeout, which can be disabled by" +
        " setting to zero or negative value.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.HOURS.toMillis(6L))

  val FRONTEND_IDLE_SESSION_TIMEOUT: ConfigEntry[Long] =
    KyuubiConfigBuilder("spark.kyuubi.frontend.session.timeout")
      .doc("The check interval for session/operation timeout, which can be disabled by setting" +
        " to zero or negative value.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.HOURS.toMillis(8L))

  val FRONTEND_IDLE_SESSION_CHECK_OPERATION: ConfigEntry[Boolean] =
    KyuubiConfigBuilder("spark.kyuubi.frontend.session.check.operation")
      .doc("Session will be considered to be idle only if there is no activity, and there is no" +
        " pending operation. This setting takes effect only if session idle timeout" +
        " (spark.kyuubi.frontend.session.timeout) and checking" +
        " (spark.kyuubi.frontend.session.check.interval) are enabled.")
      .booleanConf
      .createWithDefault(true)

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //                              Frontend Service Configuration                                 //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  val FRONTEND_BIND_HOST: ConfigEntry[String] =
    KyuubiConfigBuilder("spark.kyuubi.frontend.bind.host")
      .doc("Bind host on which to run the Kyuubi Server.")
      .stringConf
      .createWithDefault(KyuubiSparkUtil.localHostName())

  val FRONTEND_BIND_PORT: ConfigEntry[Int] =
    KyuubiConfigBuilder("spark.kyuubi.frontend.bind.port")
      .doc("Bind port on which to run the Kyuubi Server.")
      .intConf
      .createWithDefault(10009)

  val FRONTEND_WORKER_KEEPALIVE_TIME: ConfigEntry[Long] =
    KyuubiConfigBuilder("spark.kyuubi.frontend.worker.keepalive.time")
      .doc("Keep-alive time (in seconds) for an idle worker thread")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(TimeUnit.SECONDS.toSeconds(60L))

  val FRONTEND_MIN_WORKER_THREADS: ConfigEntry[Int] =
    KyuubiConfigBuilder("spark.kyuubi.frontend.min.worker.threads")
      .doc("Minimum number of threads in the of frontend worker thread pool for KyuubiServer")
      .intConf
      .createWithDefault(50)

  val FRONTEND_MAX_WORKER_THREADS: ConfigEntry[Int] =
    KyuubiConfigBuilder("spark.kyuubi.frontend.max.worker.threads")
      .doc("Maximum number of threads in the of frontend worker thread pool for KyuubiServer")
      .intConf
      .createWithDefault(500)

  val FRONTEND_ALLOW_USER_SUBSTITUTION: ConfigEntry[Boolean] =
    KyuubiConfigBuilder("spark.kyuubi.frontend.allow.user.substitution")
      .doc("Allow alternate user to be specified as part of open connection request.")
      .booleanConf
      .createWithDefault(true)

  val FRONTEND_ENABLE_DOAS: ConfigEntry[Boolean] =
    KyuubiConfigBuilder("spark.kyuubi.frontend.enable.doAs")
      .doc("Setting this property to true enables executing operations as the user making the" +
        " calls to it.")
      .booleanConf
      .createWithDefault(true)

  val FRONTEND_MAX_MESSAGE_SIZE: ConfigEntry[Int] =
    KyuubiConfigBuilder("spark.kyuubi.frontend.max.message.size")
      .doc("Maximum message size in bytes a Kyuubi server will accept.")
      .intConf
      .createWithDefault(104857600)

  val FRONTEND_LOGIN_TIMEOUT: ConfigEntry[Long] =
    KyuubiConfigBuilder("spark.kyuubi.frontend.login.timeout")
      .doc("Timeout for Thrift clients during login to Kyuubi Server.")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(TimeUnit.SECONDS.toSeconds(20L))

  val FRONTEND_LOGIN_BEBACKOFF_SLOT_LENGTH: ConfigEntry[Long] =
    KyuubiConfigBuilder("spark.kyuubi.frontend.backoff.slot.length")
      .doc("Time to back off during login to Kyuubi Server.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.MILLISECONDS.toSeconds(100L))

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //                                        SparkSession                                         //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  val BACKEND_SESSION_WAIT_OTHER_TIMES: ConfigEntry[Int] =
    KyuubiConfigBuilder("spark.kyuubi.backend.session.wait.other.times")
      .doc("How many times to check when another session with the same user is initializing " +
        "SparkContext. Total Time will be times by " +
        "`spark.kyuubi.backend.session.wait.other.interval`")
      .intConf
      .createWithDefault(60)

  val BACKEND_SESSION_WAIT_OTHER_INTERVAL: ConfigEntry[Long] =
    KyuubiConfigBuilder("spark.kyuubi.backend.session.wait.other.interval")
      .doc("The interval for checking whether other thread with the same user has completed" +
      " SparkContext instantiation.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.SECONDS.toMillis(1L))

  val BACKEND_SESSION_INIT_TIMEOUT: ConfigEntry[Long] =
    KyuubiConfigBuilder("spark.kyuubi.backend.session.init.timeout")
      .doc("How long we suggest the server to give up instantiating SparkContext")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(TimeUnit.SECONDS.toSeconds(60L))

  val BACKEND_SESSION_CHECK_INTERVAL: ConfigEntry[Long] =
    KyuubiConfigBuilder("spark.kyuubi.backend.session.check.interval")
      .doc("The check interval for backend session a.k.a SparkSession timeout")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(TimeUnit.MINUTES.toSeconds(1L))

  val BACKEND_SESSION_IDLE_TIMEOUT: ConfigEntry[Long] =
    KyuubiConfigBuilder("spark.kyuubi.backend.session.idle.timeout")
      .doc("SparkSession timeout")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.MINUTES.toMillis(30L))

  val BACKEND_SESSION_LOCAL_DIR: ConfigEntry[String] =
    KyuubiConfigBuilder("spark.kyuubi.backend.session.local.dir")
      .doc("Default value to set `spark.local.dir`, for YARN mode, this only affect the Kyuubi" +
        " server side settings according to the rule of Spark treating `spark.local.dir`")
      .stringConf
      .createWithDefault(
        s"${sys.env.getOrElse("KYUUBI_HOME", System.getProperty("java.io.tmpdir"))}"
        + File.separator + "local")

  val BACKEND_SESSION_LONG_CACHE: ConfigEntry[Boolean] =
  KyuubiConfigBuilder("spark.kyuubi.backend.session.long.cache")
    .doc("Whether to update the tokens of Spark's executor to support long caching SparkSessions" +
      " iff true && `spark.kyuubi.backend.token.update.class` is loadable. This is used towards" +
      " kerberized hadoop clusters in case of `spark.kyuubi.backend.session.idle.timeout` is" +
      " set longer than token expiration time limit or SparkSession never idles. ")
    .booleanConf
    .createWithDefault(UserGroupInformation.isSecurityEnabled)

  val BACKEND_SESSION_TOKEN_UPDATE_CLASS: ConfigEntry[String] =
    KyuubiConfigBuilder("spark.kyuubi.backend.token.update.class")
      .doc("`CoarseGrainedClusterMessages` for token update message from the driver of Spark to" +
        " executors, it is loadable only by higher version Spark release(2.3 and later)")
      .stringConf
      .createWithDefault(
        "org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages$UpdateDelegationTokens")

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //                                      Authentication                                         //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  val AUTHENTICATION_METHOD: ConfigEntry[String] =
    KyuubiConfigBuilder("spark.kyuubi.authentication")
      .doc("Client authentication types." +
        " NONE: no authentication check." +
        " KERBEROS: Kerberos/GSSAPI authentication." +
        " LDAP: Lightweight Directory Access Protocol authentication.")
      .stringConf
      .createWithDefault("NONE")

  val SASL_QOP: ConfigEntry[String] =
    KyuubiConfigBuilder("spark.kyuubi.sasl.qop")
      .doc("Sasl QOP enable higher levels of protection for Kyuubi communication with clients." +
        " auth - authentication only (default)" +
        " auth-int - authentication plus integrity protection" +
        " auth-conf - authentication plus integrity and confidentiality protectionThis is" +
        " applicable only if Kyuubi is configured to use Kerberos authentication.")
      .stringConf
      .createWithDefault("auth")

  val AUTHENTICATION_LDAP_URL: ConfigEntry[String] =
    KyuubiConfigBuilder("spark.kyuubi.authentication.ldap.url")
      .doc("SPACE character separated LDAP connection URL(s).")
      .stringConf
      .createWithDefault("")

  val AUTHENTICATION_LDAP_BASEDN: ConfigEntry[String] =
    KyuubiConfigBuilder("spark.kyuubi.authentication.ldap.baseDN")
      .doc("LDAP base DN.")
      .stringConf
      .createWithDefault("")

  val AUTHENTICATION_LDAP_DOMAIN: ConfigEntry[String] =
    KyuubiConfigBuilder("spark.kyuubi.authentication.ldap.Domain")
      .doc("LDAP base DN.")
      .stringConf
      .createWithDefault("")

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //                                      Authorization                                          //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  val AUTHORIZATION_METHOD: ConfigEntry[String] =
  KyuubiConfigBuilder("spark.kyuubi.authorization.class")
    .doc("A Rule[LogicalPlan] to support Kyuubi with Authorization.")
    .stringConf
    .createWithDefault("org.apache.spark.sql.catalyst.optimizer.Authorizer")

  val AUTHORIZATION_ENABLE: ConfigEntry[Boolean] =
    KyuubiConfigBuilder("spark.kyuubi.authorization.enabled")
      .doc("When true, Kyuubi authorization is enabled.")
      .booleanConf
      .createWithDefault(false)

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //                                         Operation                                           //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  val OPERATION_IDLE_TIMEOUT: ConfigEntry[Long] =
    KyuubiConfigBuilder("spark.kyuubi.operation.idle.timeout")
      .doc("Operation will be closed when it's not accessed for this duration of time")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.HOURS.toMillis(6L))

  val OPERATION_INCREMENTAL_COLLECT: ConfigEntry[Boolean] =
    KyuubiConfigBuilder("spark.kyuubi.operation.incremental.collect")
      .doc("Whether to use incremental result collection from Spark executor side to Kyuubi" +
        " server side")
      .booleanConf
    .createWithDefault(false)

  val OPERATION_RESULT_LIMIT: ConfigEntry[Int] =
    KyuubiConfigBuilder("spark.kyuubi.operation.result.limit")
      .doc("In non-incremental result collection mode, set this to a positive value to limit the" +
        " size of result collected to driver side.")
      .intConf
      .createWithDefault(-1)

  val OPERATION_DOWNLOADED_RESOURCES_DIR: ConfigEntry[String] = {
    KyuubiConfigBuilder("spark.kyuubi.operation.downloaded.resources.dir")
      .doc("Temporary local directory for added resources in the remote file system.")
      .stringConf
      .createWithDefault(
        s"${sys.env.getOrElse("KYUUBI_HOME", System.getProperty("java.io.tmpdir"))}"
          + File.separator + "resources")
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //                                   Containerization                                          //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  val YARN_CONTAINER_TIMEOUT: ConfigEntry[Long] =
    KyuubiConfigBuilder("spark.kyuubi.yarn.container.timeout")
      .doc("Timeout for client to wait Kyuubi successfully initialising itself")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.SECONDS.toMillis(60L))

  /**
   * Return all the configuration definitions that have been defined in [[KyuubiConf]]. Each
   * definition contains key, defaultValue.
   */
  def getAllDefaults: Map[String, String] = {
    kyuubiConfEntries.entrySet().asScala.map {kv =>
      (kv.getKey, kv.getValue.defaultValueString)
    }.toMap
  }

  implicit def convertBooleanConf(config: ConfigEntry[Boolean]): String = config.key
  implicit def convertIntConf(config: ConfigEntry[Int]): String = config.key
  implicit def convertLongConf(config: ConfigEntry[Long]): String = config.key
  implicit def convertStringConf(config: ConfigEntry[String]): String = config.key
}
