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
import java.util.HashMap
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}

/**
 * Kyuubi server level configuration which will be set when at the very beginning of server start.
 *
 */
object KyuubiConf {

  private[this] val kyuubiConfEntries = new HashMap[String, ConfigEntry[_]]()

  def register(entry: ConfigEntry[_]): Unit = {
    require(!kyuubiConfEntries.containsKey(entry.key),
      s"Duplicate SQLConfigEntry. ${entry.key} has been registered")
    kyuubiConfEntries.put(entry.key, entry)
  }

  private[this] object KyuubiConfigBuilder {
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
        " in spark.kyuubi.zookeeper.quorum does not contain port numbers, this value is used")
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
      .doc("max retry time connecting to the zk server")
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
        s"${sys.env.getOrElse("SPARK_LOG_DIR",
          sys.env.getOrElse("SPARK_HOME", System.getProperty("java.io.tmpdir")))}"
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
      .doc("Time that an idle KyuubiServer async thread (from the thread pool) will wait for" +
        " a new task to arrive before terminating")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.SECONDS.toMillis(10L))

  val ASYNC_EXEC_SHUTDOWN_TIMEOUT: ConfigEntry[Long] =
    KyuubiConfigBuilder("spark.kyuubi.async.exec.shutdown.timeout")
      .doc("How long KyuubiServer shutdown will wait for async threads to terminate.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.SECONDS.toMillis(10L))

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //                                    Session Idle Check                                       //
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
        " (spark.kyuubi.idle.session.timeout) and checking (spark.kyuubi.session.check.interval)" +
        " are enabled.")
      .booleanConf
      .createWithDefault(true)

  val BACKEND_SESSION_CHECK_INTERVAL: ConfigEntry[Long] =
    KyuubiConfigBuilder("spark.kyuubi.backend.session.check.interval")
      .doc("The check interval for backend session a.k.a SparkSession timeout")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.MINUTES.toMillis(20L))

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //                                   On Spark Session Init                                     //
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
      .doc("")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.SECONDS.toMillis(1L))

  val BACKEND_SESSTION_INIT_TIMEOUT =
    KyuubiConfigBuilder("spark.kyuubi.backend.session.init.timeout")
    .doc("")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefault(TimeUnit.SECONDS.toSeconds(60L))

  /**
   * Return all the configuration definitions that have been defined in [[KyuubiConf]]. Each
   * definition contains key, defaultValue.
   */
  def getAllDefaults: Map[String, String] = {
    kyuubiConfEntries.entrySet().asScala.map {kv =>
      (kv.getKey, kv.getValue.defaultValueString)
    }.toMap
  }
}
