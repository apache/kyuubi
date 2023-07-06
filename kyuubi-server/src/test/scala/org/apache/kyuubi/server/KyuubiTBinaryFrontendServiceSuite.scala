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

import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.{TOpenSessionReq, TSessionHandle}
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{KyuubiFunSuite, Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.EngineRef
import org.apache.kyuubi.engine.ShareLevel.CONNECTION
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.operation.TClientTestUtils
import org.apache.kyuubi.plugin.PluginLoader
import org.apache.kyuubi.session.{KyuubiSessionImpl, KyuubiSessionManager}

class KyuubiTBinaryFrontendServiceSuite extends WithKyuubiServer with KyuubiFunSuite {

  override protected val conf: KyuubiConf = KyuubiConf()

  override def afterEach(): Unit = {
    val sessionManager = server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]
    sessionManager.allSessions().foreach { session =>
      sessionManager.closeSession(session.handle)
    }
  }

  test("connection metrics") {
    val totalConnections =
      MetricsSystem.counterValue(MetricsConstants.THRIFT_BINARY_CONN_TOTAL).getOrElse(0L)
    val openConnections =
      MetricsSystem.counterValue(MetricsConstants.THRIFT_BINARY_CONN_OPEN).getOrElse(0L)

    TClientTestUtils.withThriftClient(server.frontendServices.head) {
      client =>
        val req = new TOpenSessionReq()
        req.setUsername(Utils.currentUser)
        req.setPassword("anonymous")
        client.OpenSession(req)

        assert(MetricsSystem.counterValue(
          MetricsConstants.THRIFT_BINARY_CONN_TOTAL).getOrElse(0L) - totalConnections === 1)
        assert(MetricsSystem.counterValue(
          MetricsConstants.THRIFT_BINARY_CONN_OPEN).getOrElse(0L) - openConnections === 1)

        TClientTestUtils.withThriftClient(server.frontendServices.head) {
          client =>
            val req = new TOpenSessionReq()
            req.setUsername(Utils.currentUser)
            req.setPassword("anonymous")
            client.OpenSession(req)

            assert(MetricsSystem.counterValue(
              MetricsConstants.THRIFT_BINARY_CONN_TOTAL).getOrElse(0L) - totalConnections
              === 2)
            assert(MetricsSystem.counterValue(
              MetricsConstants.THRIFT_BINARY_CONN_OPEN).getOrElse(0L) - openConnections
              === 2)
        }

        eventually(timeout(1.seconds), interval(200.milliseconds)) {
          assert(MetricsSystem.counterValue(
            MetricsConstants.THRIFT_BINARY_CONN_TOTAL).getOrElse(0L) - totalConnections
            === 2)
          assert(MetricsSystem.counterValue(
            MetricsConstants.THRIFT_BINARY_CONN_OPEN).getOrElse(0L) - openConnections === 1)
        }
    }

    eventually(timeout(1.seconds), interval(200.milliseconds)) {
      assert(MetricsSystem.counterValue(
        MetricsConstants.THRIFT_BINARY_CONN_TOTAL).getOrElse(0L) - totalConnections
        === 2)
      assert(MetricsSystem.counterValue(
        MetricsConstants.THRIFT_BINARY_CONN_OPEN).getOrElse(0L) - openConnections === 0)
    }
  }

  test("do not close session when disconnect") {
    val sessionCount = server.backendService.sessionManager.allSessions().size
    var handle: TSessionHandle = null
    TClientTestUtils.withThriftClient(server.frontendServices.head) {
      client =>
        val req = new TOpenSessionReq()
        req.setUsername(Utils.currentUser)
        req.setPassword("anonymous")
        req.setConfiguration(Map("kyuubi.session.close.on.disconnect" -> "false").asJava)
        val resp = client.OpenSession(req)
        handle = resp.getSessionHandle

        assert(server.backendService.sessionManager.allSessions().size - sessionCount == 1)
    }
    Thread.sleep(3000L)
    assert(server.backendService.sessionManager.allSessions().size - sessionCount == 1)
  }

  test("close session when disconnect - default behavior") {
    val sessionCount = server.backendService.sessionManager.allSessions().size
    var handle: TSessionHandle = null
    TClientTestUtils.withThriftClient(server.frontendServices.head) {
      client =>
        val req = new TOpenSessionReq()
        req.setUsername(Utils.currentUser)
        req.setPassword("anonymous")
        val resp = client.OpenSession(req)
        handle = resp.getSessionHandle

        assert(server.backendService.sessionManager.allSessions().size - sessionCount == 1)
    }
    Thread.sleep(3000L)
    assert(server.backendService.sessionManager.allSessions().size == sessionCount)
  }

  test("open session with engineRefId") {
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, CONNECTION.toString)
    val engineRefId = UUID.randomUUID().toString
    val engine = new EngineRef(
      conf.clone,
      Utils.currentUser,
      PluginLoader.loadGroupProvider(conf),
      engineRefId,
      null)

    withDiscoveryClient(conf) { client =>
      engine.getOrCreate(client)
    }

    var handle: TSessionHandle = null
    TClientTestUtils.withThriftClient(server.frontendServices.head) {
      client =>
        val req = new TOpenSessionReq()
        req.setUsername(Utils.currentUser)
        req.setPassword("anonymous")
        req.setConfiguration(Map("kyuubi.engine.refId" -> engineRefId).asJava)
        val resp = client.OpenSession(req)
        handle = resp.getSessionHandle

        val sessionManager = server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]
        assert(sessionManager.allSessions().size === 1)
        val engineId = sessionManager.allSessions().head.asInstanceOf[KyuubiSessionImpl]
          .engine.getEngineRefId()
        assert(engineId == engineRefId)
    }
  }
}
