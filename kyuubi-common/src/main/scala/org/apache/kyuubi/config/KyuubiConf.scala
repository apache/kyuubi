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

package org.apache.kyuubi.config

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.kyuubi.{Logging, Utils}

case class KyuubiConf(loadSysDefault: Boolean = true) extends Logging {
  private val settings = new ConcurrentHashMap[String, String]()
  private lazy val reader: ConfigProvider = new ConfigProvider(settings)

  if (loadSysDefault) {
    loadFromMap()
  }

  private def loadFromMap(props: Map[String, String] = Utils.getSystemProperties): KyuubiConf = {
    for ((key, value) <- props if key.startsWith(KYUUBI_PREFIX)) {
      set(key, value)
    }
    this
  }

  def loadFileDefaults(): KyuubiConf = {
    val maybeConfigFile = Utils.getDefaultPropertiesFile()
    loadFromMap(Utils.getPropertiesFromFile(maybeConfigFile))
    this
  }

  def set[T](entry: ConfigEntry[T], value: T): KyuubiConf = {
    settings.put(entry.key, entry.strConverter(value))
    this
  }

  def set[T](entry: OptionalConfigEntry[T], value: T): KyuubiConf = {
    set(entry.key, entry.rawStrConverter(value))
    this
  }

  def set(key: String, value: String): KyuubiConf = {
    require(key != null)
    require(value != null)
    settings.put(key, value)
    this
  }

  def setIfMissing[T](entry: ConfigEntry[T], value: T): KyuubiConf = {
    settings.putIfAbsent(entry.key, entry.strConverter(value))
    this
  }

  def get[T](config: ConfigEntry[T]): T = {
    config.readFrom(reader)
  }

  /** unset a parameter from the configuration */
  def unset(key: String): KyuubiConf = {
    settings.remove(key)
    this
  }

  def unset(entry: ConfigEntry[_]): KyuubiConf = {
    unset(entry.key)
  }

  /** Get all parameters as map */
  def getAll: Map[String, String] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toMap[String, String]
  }

  /**
   * Retrieve key-value pairs from [[KyuubiConf]] starting with `dropped.remainder`, and put them to
   * the result map with the `dropped` of key being dropped.
   * @param dropped first part of prefix which will dropped for the new key
   * @param remainder second part of the prefix which will be remained in the key
   */
  def getAllWithPrefix(dropped: String, remainder: String): Map[String, String] = {
    getAll.filter { case (k, _) => k.startsWith(s"$dropped.$remainder")}.map {
      case (k, v) => (k.substring(dropped.length), v)
    }
  }
}

object KyuubiConf {

  /** a custom directory that contains the [[KYUUBI_CONF_FILE_NAME]] */
  final val KYUUBI_CONF_DIR = "KYUUBI_CONF_DIR"
  /** the default file that contains kyuubi properties */
  final val KYUUBI_CONF_FILE_NAME = "kyuubi-defaults.conf"
  final val KYUUBI_HOME = "KYUUBI_HOME"

  def buildConf(key: String): ConfigBuilder = ConfigBuilder(KYUUBI_PREFIX + key)

  val EMBEDDED_ZK_PORT: ConfigEntry[Int] = buildConf("embedded.zookeeper.port")
    .doc("The port of the embedded zookeeper server")
    .version("1.0.0")
    .intConf
    .createWithDefault(2181)

  val EMBEDDED_ZK_TEMP_DIR: ConfigEntry[String] = buildConf("embedded.zookeeper.directory")
    .doc("The temporary directory for the embedded zookeeper server")
    .version("1.0.0")
    .stringConf
    .createWithDefault(Utils.resolveURI("embedded_zookeeper").getRawPath)

  val SERVER_PRINCIPAL: OptionalConfigEntry[String] = buildConf("server.principal")
    .doc("")
    .version("1.0.0")
    .stringConf
    .createOptional

  val SERVER_KEYTAB: OptionalConfigEntry[String] = buildConf("server.keytab")
    .doc("")
    .version("1.0.0")
    .stringConf
    .createOptional

  val OPERATION_IDLE_TIMEOUT: ConfigEntry[Long] = buildConf("operation.idle.timeout")
    .doc("Operation will be closed when it's not accessed for this duration of time")
    .version("1.0.0")
    .timeConf
    .createWithDefault(Duration.ofHours(3).toMillis)


  /////////////////////////////////////////////////////////////////////////////////////////////////
  //                              Frontend Service Configuration                                 //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  val FRONTEND_BIND_HOST: OptionalConfigEntry[String] = buildConf("frontend.bind.host")
    .doc("Hostname or IP of the machine on which to run the frontend service.")
    .version("1.0.0")
    .stringConf
    .createOptional

  val FRONTEND_BIND_PORT: ConfigEntry[Int] = buildConf("frontend.bind.port")
    .doc("Port of the machine on which to run the frontend service.")
    .version("1.0.0")
    .intConf
    .checkValue(p => p == 0 || (p > 1024 && p < 65535), "Invalid Port number")
    .createWithDefault(10009)

  val FRONTEND_MIN_WORKER_THREADS: ConfigEntry[Int] = buildConf("frontend.min.worker.threads")
    .doc("Minimum number of threads in the of frontend worker thread pool for the frontend" +
      " service")
    .version("1.0.0")
    .intConf
    .createWithDefault(9)

  val FRONTEND_MAX_WORKER_THREADS: ConfigEntry[Int] = buildConf("frontend.max.worker.threads")
    .doc("Maximum number of threads in the of frontend worker thread pool for the frontend" +
      " service")
    .version("1.0.0")
    .intConf
    .createWithDefault(99)

  val FRONTEND_WORKER_KEEPALIVE_TIME: ConfigEntry[Long] =
    buildConf("frontend.worker.keepalive.time")
      .doc("Keep-alive time (in milliseconds) for an idle worker thread")
      .version("1.0.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(60).toMillis)

  val FRONTEND_MAX_MESSAGE_SIZE: ConfigEntry[Int] =
    buildConf("frontend.max.message.size")
      .doc("Maximum message size in bytes a Kyuubi server will accept.")
      .intConf
      .createWithDefault(104857600)

  val FRONTEND_LOGIN_TIMEOUT: ConfigEntry[Long] =
    buildConf("frontend.login.timeout")
      .doc("Timeout for Thrift clients during login to the frontend service.")
      .timeConf
      .createWithDefault(Duration.ofSeconds(20).toMillis)

  val FRONTEND_LOGIN_BACKOFF_SLOT_LENGTH: ConfigEntry[Long] =
    buildConf("frontend.backoff.slot.length")
      .doc("Time to back off during login to the frontend service.")
      .timeConf
      .createWithDefault(Duration.ofMillis(100).toMillis)
}
