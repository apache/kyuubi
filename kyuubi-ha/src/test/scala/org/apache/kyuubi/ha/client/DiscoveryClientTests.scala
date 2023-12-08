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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiFunSuite, KyuubiSQLException}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ADDRESSES, HA_NAMESPACE}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.service._

trait DiscoveryClientTests extends KyuubiFunSuite {

  protected def conf: KyuubiConf

  protected def getConnectString: String

  test("publish instance to discovery service") {
    val namespace = "kyuubiserver"

    conf
      .set(HA_ADDRESSES, getConnectString)
      .set(HA_NAMESPACE, namespace)
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

    var discovery: ServiceDiscovery = null
    val service: Serverable = new NoopTBinaryFrontendServer() {
      override val frontendServices: Seq[NoopTBinaryFrontendService] = Seq(
        new NoopTBinaryFrontendService(this) {
          override val discoveryService: Option[Service] = {
            discovery = new KyuubiServiceDiscovery(this)
            Some(discovery)
          }
        })
    }
    service.initialize(conf)
    service.start()
    val basePath = s"/$namespace"
    try {
      withDiscoveryClient(conf) { discoveryClient =>
        assert(discoveryClient.pathNonExists("/abc"))
        assert(discoveryClient.pathExists(basePath))
        val children = discoveryClient.getChildren(basePath)
        assert(children.head ===
          s"serverUri=${service.frontendServices.head.connectionUrl};" +
          s"version=$KYUUBI_VERSION;sequence=0000000000")

        children.foreach { child =>
          discoveryClient.delete(s"$basePath/$child")
        }
        eventually(timeout(5.seconds), interval(100.millis)) {
          assert(discovery.getServiceState === ServiceState.STOPPED)
          assert(service.getServiceState === ServiceState.STOPPED)
        }
      }
    } finally {
      service.stop()
      discovery.stop()
    }
  }

  test("KYUUBI #304: Stop engine service gracefully when related node is deleted") {
    val logAppender = new LogAppender("test stop engine gracefully")
    withLogAppender(logAppender) {
      val namespace = "kyuubiengine"

      conf
        .set(HA_ADDRESSES, getConnectString)
        .set(HA_NAMESPACE, namespace)
        .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

      var discovery: ServiceDiscovery = null
      val service: Serverable = new NoopTBinaryFrontendServer() {
        override val frontendServices: Seq[NoopTBinaryFrontendService] = Seq(
          new NoopTBinaryFrontendService(this) {
            override val discoveryService: Option[Service] = {
              discovery = new KyuubiServiceDiscovery(this)
              Some(discovery)
            }
          })
      }
      service.initialize(conf)
      service.start()

      val basePath = s"/$namespace"
      try {
        withDiscoveryClient(conf) { discoveryClient =>
          assert(discoveryClient.pathNonExists("/abc"))
          assert(discoveryClient.pathExists(basePath))
          val children = discoveryClient.getChildren(basePath)
          assert(children.head ===
            s"serverUri=${service.frontendServices.head.connectionUrl};" +
            s"version=$KYUUBI_VERSION;sequence=0000000000")

          children.foreach { child =>
            discoveryClient.delete(s"""$basePath/$child""")
          }
          eventually(timeout(5.seconds), interval(100.millis)) {
            assert(discovery.getServiceState === ServiceState.STOPPED)
            assert(service.getServiceState === ServiceState.STOPPED)
            val msg = s"This Kyuubi instance ${service.frontendServices.head.connectionUrl}" +
              s" is now de-registered"
            assert(logAppender.loggingEvents.exists(
              _.getMessage.getFormattedMessage.contains(msg)))
          }
        }
      } finally {
        service.stop()
        discovery.stop()
      }
    }
  }

  test("distribute lock") {
    val lockPath = "/lock-test"
    val lockLatch = new CountDownLatch(1)

    new Thread(() => {
      withDiscoveryClient(conf) { discoveryClient =>
        discoveryClient.tryWithLock(lockPath, 10000) {
          lockLatch.countDown()
          Thread.sleep(15000)
        }
      }
    }).start()

    withDiscoveryClient(conf) { discoveryClient =>
      assert(lockLatch.await(20000, TimeUnit.MILLISECONDS))
      val e = intercept[KyuubiSQLException] {
        discoveryClient.tryWithLock(lockPath, 5000) {}
      }
      assert(e.getMessage contains s"Timeout to lock on path [$lockPath]")
    }
  }

  test("getAndIncrement method test") {
    withDiscoveryClient(conf) { discoveryClient =>
      (0 until 10).foreach { i =>
        val ii = discoveryClient.getAndIncrement("/get_and_increment_test")
        assert(i == ii)
      }
    }
  }

  test("setData method test") {
    withDiscoveryClient(conf) { discoveryClient =>
      val data = "abc"
      val path = "/setData_test"
      discoveryClient.create(path, "PERSISTENT")
      discoveryClient.setData(path, data.getBytes)
      val dataFromGet = new String(discoveryClient.getData(path))
      assert(data == dataFromGet)
    }
  }
}
