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

import org.apache.kyuubi.Utils

case class KyuubiConf(loadSysDefault: Boolean = true) {
  private val settings = new ConcurrentHashMap[String, String]()
  private lazy val reader: ConfigProvider = new ConfigProvider(settings)

  if (loadSysDefault) {
    loadSysProps()
  }

  private def loadSysProps(): KyuubiConf = {
    for ((key, value) <- Utils.getSystemProperties if key.startsWith(KYUUBI_PREFIX)) {
      set(key, value)
    }
    this
  }

  def set[T](entry: ConfigEntry[T], value: T): KyuubiConf = {
    settings.put(entry.key, entry.strConverter(value))
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

}

object KyuubiConf {

  /** a custom directory that contains the [[KYUUBI_CONF_FILE_NAME]] */
  final val KYUUBI_CONF_DIR = "KYUUBI_CONF_DIR"
  /** the default file that contains kyuubi properties */
  final val KYUUBI_CONF_FILE_NAME = "kyuubi-defaults.conf"
  final val KYUUBI_HOME = "KYUUBI_HOME"

  def buildConf(key: String): ConfigBuilder = ConfigBuilder(KYUUBI_PREFIX + key)

  val EMBEDDED_ZK_PORT: ConfigEntry[Int] =
    buildConf("embedded.zk.port")
      .doc("The port of the embedded zookeeper server")
      .version("1.0.0")
      .intConf.checkValue(_ >= 0, s"The value of $EMBEDDED_ZK_PORT must be >= 0")
      .createWithDefault(2181)

  val EMBEDDED_ZK_TEMP_DIR: ConfigEntry[String] =
    buildConf("embedded.zk.directory")
    .doc("The temporary directory for the embedded zookeeper server")
    .version("1.0.0")
    .stringConf
    .createWithDefault(Utils.resolveURI("embedded_zookeeper").getRawPath)

  val HA_ZK_QUORUM: OptionalConfigEntry[Seq[String]] =
    buildConf("ha.zk.quorum")
      .version("1.0.0")
      .stringConf
      .toSequence
      .createOptional
}
