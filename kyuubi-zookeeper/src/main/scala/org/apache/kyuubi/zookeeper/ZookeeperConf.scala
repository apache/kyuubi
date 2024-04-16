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

package org.apache.kyuubi.zookeeper

import org.apache.kyuubi.config.{ConfigEntry, OptionalConfigEntry}
import org.apache.kyuubi.config.KyuubiConf.buildConf

object ZookeeperConf {

  @deprecated("using kyuubi.zookeeper.embedded.client.port instead", since = "1.2.0")
  val EMBEDDED_ZK_PORT: ConfigEntry[Int] = buildConf("kyuubi.zookeeper.embedded.port")
    .doc("(deprecated) The port of the embedded ZooKeeper server")
    .version("1.0.0")
    .intConf
    .createWithDefault(2181)

  @deprecated("using kyuubi.zookeeper.embedded.data.dir instead", since = "1.2.0")
  val EMBEDDED_ZK_TEMP_DIR: ConfigEntry[String] = buildConf("kyuubi.zookeeper.embedded.directory")
    .doc("(deprecated) The temporary directory for the embedded ZooKeeper server. " +
      "If it is a relative path, it is resolved relative to KYUUBI_HOME. ")
    .version("1.0.0")
    .stringConf
    .createWithDefault("embedded_zookeeper")

  val ZK_CLIENT_PORT: ConfigEntry[Int] = buildConf("kyuubi.zookeeper.embedded.client.port")
    .doc("clientPort for the embedded ZooKeeper server to listen for client connections," +
      " a client here could be Kyuubi server, engine, and JDBC client")
    .version("1.2.0")
    .fallbackConf(EMBEDDED_ZK_PORT)

  val ZK_CLIENT_PORT_ADDRESS: OptionalConfigEntry[String] =
    buildConf("kyuubi.zookeeper.embedded.client.port.address")
      .doc("clientPortAddress for the embedded ZooKeeper server to")
      .version("1.2.0")
      .stringConf
      .createOptional

  val ZK_CLIENT_USE_HOSTNAME: ConfigEntry[Boolean] =
    buildConf("kyuubi.zookeeper.embedded.client.use.hostname")
      .doc("When true, embedded Zookeeper prefer to bind hostname, otherwise, ip address.")
      .version("1.7.2")
      .booleanConf
      .createWithDefault(false)

  val ZK_DATA_DIR: ConfigEntry[String] = buildConf("kyuubi.zookeeper.embedded.data.dir")
    .doc("dataDir for the embedded zookeeper server where stores the in-memory database" +
      " snapshots and, unless specified otherwise, the transaction log of updates to the" +
      " database. If it is a relative path, it is resolved relative to KYUUBI_HOME.")
    .version("1.2.0")
    .fallbackConf(EMBEDDED_ZK_TEMP_DIR)

  val ZK_DATA_LOG_DIR: ConfigEntry[String] = buildConf("kyuubi.zookeeper.embedded.data.log.dir")
    .doc("dataLogDir for the embedded ZooKeeper server where writes the transaction log. " +
      "If it is a relative path, it is resolved relative to KYUUBI_HOME.")
    .version("1.2.0")
    .fallbackConf(ZK_DATA_DIR)

  val ZK_TICK_TIME: ConfigEntry[Int] = buildConf("kyuubi.zookeeper.embedded.tick.time")
    .doc("tickTime in milliseconds for the embedded ZooKeeper server")
    .version("1.2.0")
    .intConf
    .createWithDefault(3000)

  val ZK_MAX_CLIENT_CONNECTIONS: ConfigEntry[Int] =
    buildConf("kyuubi.zookeeper.embedded.max.client.connections")
      .doc("maxClientCnxns for the embedded ZooKeeper server to limit the number of concurrent" +
        " connections of a single client identified by IP address")
      .version("1.2.0")
      .intConf
      .createWithDefault(120)

  val ZK_MIN_SESSION_TIMEOUT: ConfigEntry[Int] =
    buildConf("kyuubi.zookeeper.embedded.min.session.timeout")
      .doc("minSessionTimeout in milliseconds for the embedded ZooKeeper server will allow the" +
        " client to negotiate. Defaults to 2 times the tickTime")
      .version("1.2.0")
      .intConf
      .createWithDefault(3000 * 2)

  val ZK_MAX_SESSION_TIMEOUT: ConfigEntry[Int] =
    buildConf("kyuubi.zookeeper.embedded.max.session.timeout")
      .doc("maxSessionTimeout in milliseconds for the embedded ZooKeeper server will allow the" +
        " client to negotiate. Defaults to 20 times the tickTime")
      .version("1.2.0")
      .intConf
      .createWithDefault(3000 * 20)
}
