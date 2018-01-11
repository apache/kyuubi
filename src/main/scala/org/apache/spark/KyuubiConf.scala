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
import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}

object KyuubiConf {

  private val kyuubiConfEntries = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, ConfigEntry[_]]())

  def register(entry: ConfigEntry[_]): Unit = kyuubiConfEntries.synchronized {
    require(!kyuubiConfEntries.containsKey(entry.key),
      s"Duplicate SQLConfigEntry. ${entry.key} has been registered")
    kyuubiConfEntries.put(entry.key, entry)
  }

  object KyuubiConfigBuilder {
    def apply(key: String): ConfigBuilder = ConfigBuilder(key).onCreate(register)
  }

  //////////////////////////////////////////////////////
  //                       ha                         //
  //////////////////////////////////////////////////////

  val SUPPORT_DYNAMIC_SERVICE_DISCOVERY =
    KyuubiConfigBuilder("spark.kyuubi.support.dynamic.service.discovery")
    .doc("Whether KyuubiServer supports dynamic service discovery for its clients." +
      " To support this, each instance of KyuubiServer currently uses ZooKeeper to" +
      " register itself, when it is brought up. JDBC/ODBC clients should use the " +
      "ZooKeeper ensemble: spark.kyuubi.zookeeper.quorum in their connection string.")
    .booleanConf
    .createWithDefault(false)

  val KYUUBI_ZOOKEEPER_QUORUM = KyuubiConfigBuilder("spark.kyuubi.zookeeper.quorum")
    .doc("Comma separated list of ZooKeeper servers to talk to, when KyuubiServer supports" +
      " service discovery via Zookeeper.")
    .stringConf
    .createWithDefault("")

  val KYUUBI_ZOOKEEPER_NAMESPACE =
    KyuubiConfigBuilder("spark.kyuubi.zookeeper.namespace")
      .doc("The parent node in ZooKeeper used by KyuubiServer when supporting dynamic service" +
        " discovery.")
      .stringConf
      .createWithDefault("kyuubiserver")

  val KYUUBI_ZOOKEEPER_CLIENT_PORT =
    KyuubiConfigBuilder("spark.kyuubi.zookeeper.client.port")
      .doc("The port of ZooKeeper servers to talk to. If the list of Zookeeper servers specified" +
        " in spark.kyuubi.zookeeper.quorum does not contain port numbers, this value is used")
      .stringConf
      .createWithDefault("2181")

  val KYUUBI_ZOOKEEPER_SESSION_TIMEOUT =
    KyuubiConfigBuilder("spark.kyuubi.zookeeper.session.timeout")
    .doc("ZooKeeper client's session timeout (in milliseconds). The client is disconnected, and" +
      " as a result, all locks released, if a heartbeat is not sent in the timeout.")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefault(TimeUnit.MINUTES.toMillis(20L))

  val KYUUBI_ZOOKEEPER_CONNECTION_BASESLEEPTIME =
    KyuubiConfigBuilder("spark.kyuubi.zookeeper.connection.basesleeptime")
      .doc("Initial amount of time (in milliseconds) to wait between retries when connecting to" +
        " the ZooKeeper server when using ExponentialBackoffRetry policy.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.SECONDS.toMillis(1L))

  val KYUUBI_ZOOKEEPER_CONNECTION_MAX_RETRIES =
    KyuubiConfigBuilder("")
      .doc("")
      .intConf
      .createWithDefault(3)

  //////////////////////////////////////////////////////
  //                      log                         //
  //////////////////////////////////////////////////////

  val KYUUBI_LOGGING_OPERATION_ENABLED =
    KyuubiConfigBuilder("spark.kyuubi.logging.operation.enabled")
      .doc("When true, KyuubiServer will save operation logs and make them available for clients")
      .booleanConf
      .createWithDefault(true)

  val KYUUBI_LOGGING_OPERATION_LOG_LOCATION =
    KyuubiConfigBuilder("spark.kyuubi.logging.operation.log.location")
      .doc("Top level directory where operation logs are stored if logging functionality is" +
        " enabled")
      .stringConf
      .createWithDefault(
        s"${sys.env.getOrElse("SPARK_LOG_DIR", System.getProperty("java.io.tmpdir"))}"
          + File.separator + "operation_logs")

  //////////////////////////////////////////////////////
  //             background exec thread               //
  //////////////////////////////////////////////////////

  val KYUUBI_ASYNC_EXEC_THREADS =
    KyuubiConfigBuilder("spark.kyuubi.async.exec.threads")
      .doc("Number of threads in the async thread pool for KyuubiServer")
      .intConf
      .createWithDefault(100)

  val KYUUBI_ASYNC_EXEC_WAIT_QUEUE_SIZE =
    KyuubiConfigBuilder("spark.kyuubi.async.exec.wait.queue.size")
      .doc("Size of the wait queue for async thread pool in KyuubiServer. After hitting this" +
        " limit, the async thread pool will reject new requests.")
      .intConf
      .createWithDefault(100)

  val KYUUBI_EXEC_KEEPALIVE_TIME =
    KyuubiConfigBuilder("spark.kyuubi.async.exec.keep.alive.time")
      .doc("Time that an idle KyuubiServer async thread (from the thread pool) will wait for" +
        " a new task to arrive before terminating")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.SECONDS.toMillis(10L))

  val KYUUBI_ASYNC_EXEC_SHUTDOWN_TIMEOUT =
    KyuubiConfigBuilder("spark.kyuubi.async.exec.shutdown.timeout")
      .doc("How long KyuubiServer shutdown will wait for async threads to terminate.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.SECONDS.toMillis(10L))

  //////////////////////////////////////////////////////
  //              checking idle session               //
  //////////////////////////////////////////////////////

  val KYUUBI_SESSION_CHECK_INTERVAL =
    KyuubiConfigBuilder("spark.kyuubi.session.check.interval")
      .doc("The check interval for session/operation timeout, which can be disabled by setting" +
        " to zero or negative value.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.HOURS.toMillis(6L))


  val KYUUBI_IDLE_SESSION_TIMEOUT =
    KyuubiConfigBuilder("spark.kyuubi.idle.session.timeout")
      .doc("The check interval for session/operation timeout, which can be disabled by setting" +
        " to zero or negative value.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.HOURS.toMillis(8L))

  val KYUUBI_IDLE_SESSION_CHECK_OPERATION =
    KyuubiConfigBuilder("spark.kyuubi.idle.session.check.operation")
      .doc("Session will be considered to be idle only if there is no activity, and there is no" +
        " pending operation. This setting takes effect only if session idle timeout" +
        " (spark.kyuubi.idle.session.timeout) and checking (spark.kyuubi.session.check.interval)" +
        " are enabled.")
      .booleanConf
      .createWithDefault(true)

  val KYUUBI_SPARK_SESSION_CHECK_INTERVAL =
    KyuubiConfigBuilder("spark.kyuubi.session.clean.interval")
      .doc("The check interval for SparkSession timeout")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.MINUTES.toMillis(20L))

  //////////////////////////////////////////////////////
  //                   security                       //
  //////////////////////////////////////////////////////

  val KYUUBI_ENABLE_DOAS =
    KyuubiConfigBuilder("spark.kyuubi.enable.doAs")
      .doc("enable proxying of running Spark")
      .booleanConf
      .createWithDefault(true)

  val KYUUBI_AUTHENTICATION =
    KyuubiConfigBuilder("spark.kyuubi.authentication")
      .doc("Client authentication types. NONE: no authentication check;" +
        " KERBEROS: Kerberos/GSSAPI authentication")
      .stringConf
      .createWithDefault("NONE")
}
