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

import java.util.concurrent.ConcurrentHashMap

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

  val HA_ZK_QUORUM: ConfigEntry[String] = buildConf("ha.zookeeper.quorum")
    .doc("The connection string for the zookeeper ensemble")
    .version("1.0.0")
    .stringConf
    .createWithDefault("")

  val HA_ZK_NAMESPACE: ConfigEntry[String] = buildConf("ha.zookeeper.namespace")
    .doc("The connection string for the zookeeper ensemble")
    .version("1.0.0")
    .stringConf
    .createWithDefault("")

  val HA_ZK_CONNECTION_MAX_RETRIES: ConfigEntry[Int] =
    buildConf("ha.zookeeper.connection.max.retries")
    .doc("Max retry times for connecting to the zookeeper ensemble")
    .version("1.0.0")
    .intConf
    .createWithDefault(3)

  val HA_ZK_CONNECTION_RETRY_WAIT: ConfigEntry[Int] =
    buildConf("ha.zookeeper.connection.retry.wait")
    .doc("Initial amount of time to wait between retries to the zookeeper ensemble")
    .version("1.0.0")
    .intConf
    .createWithDefault(1000)

  val HA_ZK_CONNECTION_TIMEOUT: ConfigEntry[Int] = buildConf("ha.zookeeper.connection.timeout")
    .doc("The timeout(ms) of creating the connection to the zookeeper ensemble")
    .version("1.0.0")
    .intConf
    .createWithDefault(60 * 1000)

  val HA_ZK_SESSION_TIMEOUT: ConfigEntry[Int] = buildConf("ha.zookeeper.session.timeout")
    .doc("The timeout(ms) of a connected session to be idled")
    .version("1.0.0")
    .intConf
    .createWithDefault(60 * 1000)

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

}
