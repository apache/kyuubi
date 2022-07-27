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

package org.apache.kyuubi.ha.client

import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.KYUUBI_VERSION
import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ADDRESSES
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_NAMESPACE
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_AUTH_TYPE
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.service.NoopTBinaryFrontendServer
import org.apache.kyuubi.service.NoopTBinaryFrontendService
import org.apache.kyuubi.service.Serverable
import org.apache.kyuubi.service.Service
import org.apache.kyuubi.service.ServiceState

trait DiscoveryClientTests extends KyuubiFunSuite {
  protected val conf: KyuubiConf

  protected def getConnectString: String

  test("publish instance to embedded zookeeper server") {
    val namespace = "kyuubiserver"

    conf
      .unset(KyuubiConf.SERVER_KEYTAB)
      .unset(KyuubiConf.SERVER_PRINCIPAL)
      .set(HA_ADDRESSES, getConnectString)
      .set(HA_NAMESPACE, namespace)
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

    var serviceDiscovery: KyuubiServiceDiscovery = null
    val server: Serverable = new NoopTBinaryFrontendServer() {
      override val frontendServices: Seq[NoopTBinaryFrontendService] = Seq(
        new NoopTBinaryFrontendService(this) {
          override val discoveryService: Option[Service] = {
            serviceDiscovery = new KyuubiServiceDiscovery(this)
            Some(serviceDiscovery)
          }
        })
    }
    server.initialize(conf)
    server.start()
    val znodeRoot = s"/$namespace"
    withDiscoveryClient(conf) { framework =>
      try {
        assert(framework.pathNonExists("/abc"))
        assert(framework.pathExists(znodeRoot))
        val children = framework.getChildren(znodeRoot)
        assert(children.head ===
          s"serviceUri=${server.frontendServices.head.connectionUrl};" +
          s"version=$KYUUBI_VERSION;sequence=0000000000")

        children.foreach { child =>
          framework.delete(s"""$znodeRoot/$child""")
        }
        eventually(timeout(5.seconds), interval(100.millis)) {
          assert(serviceDiscovery.getServiceState === ServiceState.STOPPED)
          assert(server.getServiceState === ServiceState.STOPPED)
        }
      } finally {
        server.stop()
      }
    }
  }

  test("KYUUBI-304: Stop engine service gracefully when related zk node is deleted") {
    val logAppender = new LogAppender("test stop engine gracefully")
    withLogAppender(logAppender) {
      val namespace = "kyuubiengine"

      conf
        .unset(KyuubiConf.SERVER_KEYTAB)
        .unset(KyuubiConf.SERVER_PRINCIPAL)
        .set(HA_ADDRESSES, getConnectString)
        .set(HA_NAMESPACE, namespace)
        .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
        .set(HA_ZK_AUTH_TYPE, AuthTypes.NONE.toString)

      var serviceDiscovery: KyuubiServiceDiscovery = null
      val server: Serverable = new NoopTBinaryFrontendServer() {
        override val frontendServices: Seq[NoopTBinaryFrontendService] = Seq(
          new NoopTBinaryFrontendService(this) {
            override val discoveryService: Option[Service] = {
              serviceDiscovery = new KyuubiServiceDiscovery(this)
              Some(serviceDiscovery)
            }
          })
      }
      server.initialize(conf)
      server.start()

      val znodeRoot = s"/$namespace"
      withDiscoveryClient(conf) { framework =>
        try {

          assert(framework.pathNonExists("/abc"))
          assert(framework.pathExists(znodeRoot))
          val children = framework.getChildren(znodeRoot)
          assert(children.head ===
            s"serviceUri=${server.frontendServices.head.connectionUrl};" +
            s"version=$KYUUBI_VERSION;sequence=0000000000")

          children.foreach { child =>
            framework.delete(s"""$znodeRoot/$child""")
          }
          eventually(timeout(5.seconds), interval(100.millis)) {
            assert(serviceDiscovery.getServiceState === ServiceState.STOPPED)
            assert(server.getServiceState === ServiceState.STOPPED)
            val msg = s"This Kyuubi instance ${server.frontendServices.head.connectionUrl}" +
              s" is now de-registered"
            assert(logAppender.loggingEvents.exists(
              _.getMessage.getFormattedMessage.contains(msg)))
          }
        } finally {
          server.stop()
          serviceDiscovery.stop()
        }
      }
    }
  }

  test("parse host and port from instance string") {
    val host = "127.0.0.1"
    val port = 10009
    val instance1 = s"$host:$port"
    val (host1, port1) = DiscoveryClient.parseInstanceHostPort(instance1)
    assert(host === host1)
    assert(port === port1)

    val instance2 = s"hive.server2.thrift.sasl.qop=auth;hive.server2.thrift.bind.host=$host;" +
      s"hive.server2.transport.mode=binary;hive.server2.authentication=KERBEROS;" +
      s"hive.server2.thrift.port=$port;" +
      s"hive.server2.authentication.kerberos.principal=test/_HOST@apache.org"
    val (host2, port2) = DiscoveryClient.parseInstanceHostPort(instance2)
    assert(host === host2)
    assert(port === port2)
  }
}
