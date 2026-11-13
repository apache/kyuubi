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

import java.util.UUID
import java.util.concurrent.{CountDownLatch, Executors}

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException, Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.TClientTestUtils
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TOpenSessionReq, TProtocolVersion}

class KyuubiSessionManagerLimiterSuite extends WithKyuubiServer with KyuubiFunSuite {

  private val perUserLimit = 2
  private val ipAddressLimit = 2
  private val unlimitedUser = "unlimited_user"
  private val restrictKey = "spark.sql.extensions"

  override protected val conf: KyuubiConf = KyuubiConf()
    .set(KyuubiConf.SERVER_LIMIT_CONNECTIONS_PER_USER, perUserLimit)
    .set(KyuubiConf.SERVER_LIMIT_CONNECTIONS_PER_IPADDRESS, ipAddressLimit)
    .set(KyuubiConf.SERVER_LIMIT_CONNECTIONS_USER_UNLIMITED_LIST, Set(unlimitedUser))
    .set(KyuubiConf.SESSION_CONF_RESTRICT_LIST, Set(restrictKey))
    // The sessions here only need to stay open. A Trino engine without a server url fails to
    // launch in the background before starting any process, and the session stays open.
    .set(KyuubiConf.ENGINE_TYPE, "TRINO")

  private def sessionManager: KyuubiSessionManager =
    server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]

  private def openSession(
      user: String,
      ipAddress: String,
      sessionConf: Map[String, String] = Map.empty): SessionHandle = {
    sessionManager.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
      user,
      "anonymous",
      ipAddress,
      sessionConf)
  }

  /** Fills the address with one session each of user_a and user_b, closed after `f`. */
  private def withAddressFull(ipAddress: String)(f: => Unit): Unit = {
    val sessions = Seq("user_a", "user_b").map(openSession(_, ipAddress))
    try {
      f
    } finally {
      sessions.foreach(sessionManager.closeSession)
    }
  }

  /**
   * A user of its own for every probe: a probe refused by the address limit may keep a count of
   * its user (KYUUBI #7802), which would turn the next probe of the same user away by its own
   * limit.
   */
  private def assertAddressFull(ipAddress: String): Unit = {
    val attempt = Try(openSession(s"probe_${UUID.randomUUID()}", ipAddress))
    // leave nothing behind for the next test when the assertion fails
    attempt.foreach(sessionManager.closeSession)
    assert(
      attempt.failed.toOption.exists(_.getMessage.contains("Connection limit per ipaddress")),
      s"the count of $ipAddress was given back by a session that did not hold it, got: $attempt")
  }

  test("connection limit count is given back when the session fails before it is registered") {
    // A session carrying a restricted key fails while it is created, before it is put into
    // handleToSession, so closeSession never runs for it. Unless the count openSession took is
    // given back there, every attempt keeps one, and the attempts past the limit are turned away
    // by the limiter rather than by the restrict list - the user stays locked out until the
    // server restarts.
    (1 to perUserLimit + 2).foreach { attempt =>
      TClientTestUtils.withThriftClient(server.frontendServices.head) { client =>
        val req = new TOpenSessionReq()
        req.setUsername(Utils.currentUser)
        req.setPassword("anonymous")
        req.setConfiguration(Map(restrictKey -> "org.apache.kyuubi.NoopExtensions").asJava)
        val errorMessage = client.OpenSession(req).getStatus.getErrorMessage
        assert(
          errorMessage.contains(s"$restrictKey is a restrict key"),
          s"attempt $attempt: $errorMessage")
        assert(
          !errorMessage.contains("Connection limit per user reached"),
          s"attempt $attempt: the connection limit count was not given back")
      }
    }
  }

  test("a session refused by the limiter reaches the caller as it is and leaves nothing behind") {
    val ipAddress = "10.0.1.1"
    withAddressFull(ipAddress) {
      val operationCount = sessionManager.operationManager.getOperationCount
      val caught = intercept[SessionLimiterRejectedException](openSession("probe", ipAddress))
      // not wrapped as "Error opening session ..."
      assert(caught.getMessage.startsWith("Connection limit per ipaddress reached"))
      // refused before the session is built, which would register its LaunchEngine operation
      assert(sessionManager.operationManager.getOperationCount === operationCount)
    }
  }

  test("closing a session of an unlimited user keeps the count of its address") {
    val ipAddress = "10.0.1.2"
    withAddressFull(ipAddress) {
      sessionManager.closeSession(openSession(unlimitedUser, ipAddress))
      assertAddressFull(ipAddress)
    }
  }

  test("a session of an unlimited user failing to be created keeps the count of its address") {
    val ipAddress = "10.0.1.3"
    withAddressFull(ipAddress) {
      val caught = intercept[KyuubiSQLException] {
        openSession(unlimitedUser, ipAddress, Map(restrictKey -> "org.apache.kyuubi.Noop"))
      }
      assert(caught.getMessage.contains(s"$restrictKey is a restrict key"))
      assertAddressFull(ipAddress)
    }
  }

  test("a session counted before its user became unlimited gives the count back on close") {
    val ipAddress = "10.0.1.4"
    val counted = openSession("user_a", ipAddress)
    try {
      sessionManager.refreshUnlimitedUsers(KyuubiConf(false)
        .set(KyuubiConf.SERVER_LIMIT_CONNECTIONS_USER_UNLIMITED_LIST, Set(unlimitedUser, "user_a")))
      // deciding by the unlimited list at close time would keep this count for good
      sessionManager.closeSession(counted)
    } finally {
      sessionManager.refreshUnlimitedUsers(KyuubiConf(false)
        .set(KyuubiConf.SERVER_LIMIT_CONNECTIONS_USER_UNLIMITED_LIST, Set(unlimitedUser)))
    }
    // both places of the address are free again
    withAddressFull(ipAddress) {}
  }

  test("a session closed by two callers at once gives its count back once") {
    // The idle session checker, the engine alive checker and the client may all close the same
    // session. A second give-back would take the count of another session of the address.
    val ipAddress = "10.0.1.5"
    val held = openSession("user_b", ipAddress)
    val pool = Executors.newFixedThreadPool(2)
    try {
      (1 to 100).foreach { _ =>
        val handle = openSession("user_a", ipAddress)
        val start = new CountDownLatch(1)
        val closes = (1 to 2).map { _ =>
          pool.submit(new Runnable {
            override def run(): Unit = {
              start.await()
              Try(sessionManager.closeSession(handle))
            }
          })
        }
        start.countDown()
        closes.foreach(_.get())
      }
      // user_b still holds one of the two places, so this one takes the last
      val last = openSession("user_a", ipAddress)
      try {
        assertAddressFull(ipAddress)
      } finally {
        sessionManager.closeSession(last)
      }
    } finally {
      pool.shutdownNow()
      sessionManager.closeSession(held)
    }
  }
}
