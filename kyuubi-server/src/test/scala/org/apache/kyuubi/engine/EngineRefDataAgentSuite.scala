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

package org.apache.kyuubi.engine

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ADDRESSES, HA_CLIENT_CLASS, HA_NAMESPACE}

class EngineRefDataAgentSuite extends KyuubiFunSuite {

  private val ZK_CLIENT_CLASS =
    "org.apache.kyuubi.ha.client.zookeeper.ZookeeperDiscoveryClient"

  test("derive Data Agent JDBC URL for a direct (non-HA) Kyuubi server") {
    val conf = KyuubiConf(false)
      .set(FRONTEND_ADVERTISED_HOST, "kyuubi-host")
      .set(FRONTEND_THRIFT_BINARY_BIND_PORT, 10009)
    assert(EngineRef.deriveDataAgentJdbcUrl(conf, "kyuubi") ===
      "jdbc:kyuubi://kyuubi-host:10009/default")
  }

  test("derive Data Agent JDBC URL for a ZooKeeper HA Kyuubi server uses the server namespace") {
    // HA_NAMESPACE on conf is the per-engine space (create() remaps it before the engine is built);
    // the back-connection must target the server's discovery namespace passed in as serverSpace.
    val conf = KyuubiConf(false)
      .set(HA_ADDRESSES, "zk1:2181,zk2:2181")
      .set(HA_CLIENT_CLASS, ZK_CLIENT_CLASS)
      .set(HA_NAMESPACE, "kyuubi_1.12.0_USER_DATA_AGENT_engineSpace")
    assert(EngineRef.deriveDataAgentJdbcUrl(conf, "kyuubi") ===
      "jdbc:kyuubi://zk1:2181,zk2:2181/default;" +
      "serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=kyuubi")
  }

  test("fail closed when the bind port is random (0) and the URL cannot be derived") {
    val conf = KyuubiConf(false)
      .set(FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    val e = intercept[KyuubiSQLException](EngineRef.deriveDataAgentJdbcUrl(conf, "kyuubi"))
    assert(e.getMessage.contains(FRONTEND_THRIFT_BINARY_BIND_PORT.key))
    assert(e.getMessage.contains(ENGINE_DATA_AGENT_JDBC_URL.key))
  }

  test("fail closed when Kerberos authentication is enabled") {
    // The Data Agent engine performs no Kerberos login of its own, so an auto-derived
    // back-connection would implicitly reuse the server's proxy super-user credentials.
    val conf = KyuubiConf(false)
      .set(AUTHENTICATION_METHOD, Seq("KERBEROS"))
      .set(FRONTEND_ADVERTISED_HOST, "kyuubi-host")
      .set(FRONTEND_THRIFT_BINARY_BIND_PORT, 10009)
    val e = intercept[KyuubiSQLException](EngineRef.deriveDataAgentJdbcUrl(conf, "kyuubi"))
    assert(e.getMessage.contains("Kerberos"))
    assert(e.getMessage.contains(ENGINE_DATA_AGENT_JDBC_URL.key))
  }
}
