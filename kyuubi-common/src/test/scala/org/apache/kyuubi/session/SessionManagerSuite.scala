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

package org.apache.kyuubi.session

import java.time.Duration
import java.util

import org.apache.hive.service.rpc.thrift._
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.NoopServer
import org.apache.kyuubi.service.authentication.PlainSASLHelper

class SessionManagerSuite extends KyuubiFunSuite {

  private val server = new NoopServer()
  private val conf = KyuubiConf()
    .set(KyuubiConf.FRONTEND_BIND_PORT, 0)
    .set("kyuubi.test.server.should.fail", "false")

  val user: String = System.getProperty("user.name")

  def startServer(): Unit = {
    server.initialize(conf)
    server.start()
  }

  override def afterAll(): Unit = {
    server.getServices.foreach(_.stop())
    super.afterAll()
  }

  protected def withThriftClient(f: TCLIService.Iface => Unit): Unit = {
    val hostAndPort = server.connectionUrl.split(":")
    val host = hostAndPort.head
    val port = hostAndPort(1).toInt
    val socket = new TSocket(host, port)
    val transport =
      PlainSASLHelper.getPlainTransport(Utils.currentUser, "anonymous", socket)

    val protocol = new TBinaryProtocol(transport)
    val client = new TCLIService.Client(protocol)
    transport.open()
    try {
      f(client)
    } finally {
      socket.close()
    }
  }

  protected def withSessionHandle(
    conf: java.util.Map[String, String]
  )(f: (TCLIService.Iface, TSessionHandle) => Unit): Unit = {
    withThriftClient { client =>
      val req = new TOpenSessionReq()
      req.setUsername(user)
      req.setPassword("anonymous")
      req.setConfiguration(conf)
      val resp = client.OpenSession(req)
      val handle = resp.getSessionHandle

      try {
        f(client, handle)
      } finally {
        val tCloseSessionReq = new TCloseSessionReq(handle)
        try {
           client.CloseSession(tCloseSessionReq)
        } catch {
          case e: Exception => error(s"Failed to close $handle", e)
        }
      }
    }
  }

  def waitFor(waitForTime: Long)(f: () => Boolean): Unit = {
    val startTime = System.currentTimeMillis()
    var spendTime = System.currentTimeMillis() - startTime
    while (f()) {
      spendTime = System.currentTimeMillis() - startTime
      if (spendTime > waitForTime) {
        throw new RuntimeException()
      }
    }
    info(s"Spend time $spendTime ms")
  }

  test("close expired operations") {
    conf.set(KyuubiConf.SESSION_CHECK_INTERVAL, Duration.ofSeconds(5).toMillis)
    conf.set(KyuubiConf.SESSION_TIMEOUT, Duration.ofSeconds(5).toMillis)

    val reqConf = new util.HashMap[String, String]()
    reqConf.put(
      KyuubiConf.OPERATION_IDLE_TIMEOUT.key,
      String.valueOf(Duration.ofSeconds(20).toMillis)
    )

    startServer()
    withSessionHandle(reqConf) { (client, handle) =>
      val req = new TCancelOperationReq()
      val req1 = new TGetSchemasReq(handle)
      val resp1 = client.GetSchemas(req1)

      val sessionManager = server.backendService.sessionManager
      val session = sessionManager
        .getSession(SessionHandle(handle))
        .asInstanceOf[AbstractSession]
      var lastAccessTime = session.lastAccessTime
      assert(sessionManager.getOpenSessionCount == 1)
      assert(session.lastIdleTime > 0)

      resp1.getOperationHandle
      req.setOperationHandle(resp1.getOperationHandle)
      val resp2 = client.CancelOperation(req)
      assert(resp2.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      assert(sessionManager.getOpenSessionCount == 1)
      assert(session.lastIdleTime == 0)
      assert(lastAccessTime < session.lastAccessTime)
      lastAccessTime = session.lastAccessTime

      waitFor(Duration.ofSeconds(60).toMillis) { () =>
        {
          session.lastIdleTime == 0
        }
      }

      info("operation is terminated")
      assert(lastAccessTime == session.lastAccessTime)
      assert(session.lastIdleTime > lastAccessTime)
      assert(sessionManager.getOpenSessionCount == 1)

      waitFor(Duration.ofSeconds(60).toMillis) { () =>
        lastAccessTime == session.lastAccessTime
      }
      assert(sessionManager.getOpenSessionCount == 0)
    }
  }
}
