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

package org.apache.kyuubi.service

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.{TCLIService, TCloseSessionReq, TOpenSessionReq, TSessionHandle, TStatusCode}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.PlainSASLHelper

class FrontendServiceSuite extends KyuubiFunSuite {

  private val server = new NoopServer()
  private val conf = KyuubiConf()
    .set(KyuubiConf.FRONTEND_BIND_PORT, 0)
  server.initialize(conf)
  server.start()

  val user: String = System.getProperty("user.name")

  override def afterAll(): Unit = {
    super.afterAll()
    server.stop()
  }

  protected def withThriftClient(f: TCLIService.Iface => Unit): Unit = {
    val hostAndPort = server.connectionUrl.split(":")
    val host = hostAndPort.head
    val port = hostAndPort(1).toInt
    val socket = new TSocket(host, port)
    val transport = PlainSASLHelper.getPlainTransport(Utils.currentUser, "anonymous", socket)

    val protocol = new TBinaryProtocol(transport)
    val client = new TCLIService.Client(protocol)
    transport.open()
    try {
      f(client)
    } finally {
      socket.close()
    }
  }

  protected def withSessionHandle(f: (TCLIService.Iface, TSessionHandle) => Unit): Unit = {
    withThriftClient { client =>
      val req = new TOpenSessionReq()
      req.setUsername(user)
      req.setPassword("anonymous")
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


  test("open session") {
    withThriftClient { client =>
      val req = new TOpenSessionReq()
      req.setUsername(user)
      req.setPassword("anonymous")
      val resp = client.OpenSession(req)
      val handle = resp.getSessionHandle
      assert(handle != null)
      assert(resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)

      req.setConfiguration(Map("kyuubi.test.should.fail" -> "true").asJava)
      val resp1 = client.OpenSession(req)
      assert(resp1.getSessionHandle === null)
      assert(resp1.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      val cause = KyuubiSQLException.toCause(resp1.getStatus.getInfoMessages.asScala)
      assert(cause.isInstanceOf[KyuubiSQLException])
      assert(cause.getMessage === "Asked to fail")
    }
  }
}
