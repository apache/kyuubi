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

package org.apache.kyuubi.server

import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.ha.client.ServiceDiscovery
import org.apache.kyuubi.service._
import org.apache.kyuubi.zookeeper.EmbeddedZookeeper
import org.apache.kyuubi.zookeeper.ZookeeperConf.ZK_CLIENT_PORT

class KyuubiRestServiceDiscoverySuite extends KyuubiFunSuite {

  private var zkServer: EmbeddedZookeeper = _
  private var conf: KyuubiConf = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    val embeddedZkConf = KyuubiConf()
    embeddedZkConf.set(ZK_CLIENT_PORT, 0)
    zkServer = new EmbeddedZookeeper()
    zkServer.initialize(embeddedZkConf)
    zkServer.start()
    conf = new KyuubiConf()
      .set(HA_ADDRESSES, zkServer.getConnectString)
      .set(HA_ZK_CONN_RETRY_POLICY, "ONE_TIME")
      .set(HA_ZK_CONN_BASE_RETRY_WAIT, 1000)
      .set(HA_ZK_SESSION_TIMEOUT, 3000)
      .set(HA_ZK_CONN_TIMEOUT, 3000)
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
  }

  override def afterEach(): Unit = {
    if (zkServer != null) zkServer.stop()
    super.afterEach()
  }

  test("REST discovery is conditional on HA config") {
    val noHaConf = new KyuubiConf()
    assert(!ServiceDiscovery.supportServiceDiscovery(noHaConf))
    assert(ServiceDiscovery.supportServiceDiscovery(conf))
  }

  test("publish REST instance under namespace/rest and verify with discovery client") {
    val namespace = "kyuubi-rest-test"
    conf.set(HA_NAMESPACE, namespace)

    var restDiscovery: KyuubiRestServiceDiscovery = null
    val service: Serverable = new NoopTBinaryFrontendServer() {
      override val frontendServices: Seq[NoopTBinaryFrontendService] = Seq(
        new NoopTBinaryFrontendService(this) {
          override val discoveryService: Option[Service] = {
            restDiscovery = new KyuubiRestServiceDiscovery(this)
            Some(restDiscovery)
          }
        })
    }
    service.initialize(conf)
    service.start()
    assert(service.getServiceState === ServiceState.STARTED)

    val restPath = s"/$namespace/rest"
    try {
      // Verify using the service's own discovery client
      eventually(timeout(10.seconds), interval(200.millis)) {
        withDiscoveryClient(conf) { discoveryClient =>
          assert(discoveryClient.pathExists(restPath))
          val children = discoveryClient.getChildren(restPath)
          assert(children.size === 1)
          assert(children.head.startsWith(
            s"serverUri=${service.frontendServices.head.connectionUrl}"))
        }
      }
    } finally {
      service.stop()
      restDiscovery.stop()
    }
  }

  test("graceful stop when REST service node is deleted") {
    val namespace = "kyuubi-rest-graceful"
    conf.set(HA_NAMESPACE, namespace)

    var restDiscovery: KyuubiRestServiceDiscovery = null
    val service: Serverable = new NoopTBinaryFrontendServer() {
      override val frontendServices: Seq[NoopTBinaryFrontendService] = Seq(
        new NoopTBinaryFrontendService(this) {
          override val discoveryService: Option[Service] = {
            restDiscovery = new KyuubiRestServiceDiscovery(this)
            Some(restDiscovery)
          }
        })
    }
    service.initialize(conf)
    service.start()
    assert(service.getServiceState === ServiceState.STARTED)

    val restPath = s"/$namespace/rest"
    try {
      eventually(timeout(10.seconds), interval(200.millis)) {
        withDiscoveryClient(conf) { discoveryClient =>
          assert(discoveryClient.pathExists(restPath))
        }
      }

      withDiscoveryClient(conf) { discoveryClient =>
        val children = discoveryClient.getChildren(restPath)
        children.foreach { child =>
          discoveryClient.delete(s"$restPath/$child")
        }
      }

      eventually(timeout(10.seconds), interval(100.millis)) {
        assert(restDiscovery.getServiceState === ServiceState.STOPPED)
        assert(service.getServiceState === ServiceState.STOPPED)
      }
    } finally {
      service.stop()
      if (restDiscovery != null) restDiscovery.stop()
    }
  }
}
