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

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion
import org.apache.kyuubi.sql.parser.server.KyuubiParser

class KyuubiSessionIdleTimeoutSuite extends KyuubiFunSuite {

  private def withTestSessionManager(
      launchLatch: CountDownLatch,
      failLaunch: Boolean = false)(
      f: KyuubiSessionManager => Unit): Unit = {
    val conf = KyuubiConf()
      .set(FRONTEND_PROTOCOLS, Seq(FrontendProtocols.THRIFT_BINARY.toString))
      .set(SESSION_CHECK_INTERVAL, 3100L)
      .set(SESSION_IDLE_TIMEOUT, 3000L)

    val parser = new KyuubiParser()
    val sessionManager = new KyuubiSessionManager() {
      override protected def createSession(
          protocol: TProtocolVersion,
          user: String,
          password: String,
          ipAddress: String,
          conf: Map[String, String]): Session = {
        val userConf = this.getConf.getUserDefaults(user)
        new KyuubiSessionImpl(
          protocol,
          user,
          password,
          ipAddress,
          conf,
          this,
          userConf,
          userConf.get(ENGINE_DO_AS_ENABLED),
          parser) {
          override private[kyuubi] def openEngineSession(
              extraEngineLog: Option[OperationLog] = None): Unit = {
            launchLatch.await(30, TimeUnit.SECONDS)
            if (failLaunch) {
              throw KyuubiSQLException("simulated engine launch failure")
            }
          }
        }
      }
    }

    sessionManager.initialize(conf)
    sessionManager.start()
    try {
      f(sessionManager)
    } finally {
      sessionManager.stop()
    }
  }

  test("async engine launch should not be closed by session idle timeout") {
    val launchLatch = new CountDownLatch(1)
    withTestSessionManager(launchLatch) { sessionManager =>
      val handle = sessionManager.openSession(
        TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
        "test-user",
        "anonymous",
        "127.0.0.1",
        Map(SESSION_ENGINE_LAUNCH_ASYNC.key -> "true"))

      val session = sessionManager.getSession(handle).asInstanceOf[KyuubiSessionImpl]
      assert(!session.launchEngineOp.isTerminal)

      // Wait longer than SESSION_IDLE_TIMEOUT (3s) + SESSION_CHECK_INTERVAL (3.1s) so at least
      // one timeout check runs strictly after the idle timeout has elapsed
      Thread.sleep(8000L)
      assert(session.getNoOperationTime === 0L)
      assert(!session.isClosed)
      assert(sessionManager.getActiveUserSessionCount === 1)

      // Complete engine launch and verify session is not immediately closed
      launchLatch.countDown()
      eventually(timeout(5.seconds), interval(100.milliseconds)) {
        assert(session.launchEngineOp.isTerminal)
      }
      assert(!session.isClosed)
      assert(session.getNoOperationTime < 2000L)

      // Session should still be closed after idle timeout elapses post-launch
      eventually(timeout(10.seconds), interval(200.milliseconds)) {
        assert(session.isClosed)
        assert(sessionManager.getActiveUserSessionCount === 0)
      }
    }
  }

  test("sync engine launch should not be closed by session idle timeout") {
    val launchLatch = new CountDownLatch(1)
    val executor = Executors.newSingleThreadExecutor()
    try {
      withTestSessionManager(launchLatch) { sessionManager =>
        val openFuture = executor.submit(() =>
          sessionManager.openSession(
            TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
            "test-user",
            "anonymous",
            "127.0.0.1",
            Map(SESSION_ENGINE_LAUNCH_ASYNC.key -> "false")))

        eventually(timeout(5.seconds), interval(100.milliseconds)) {
          assert(sessionManager.getActiveUserSessionCount === 1)
        }

        val session = sessionManager.allSessions().head.asInstanceOf[KyuubiSessionImpl]

        // Wait longer than SESSION_IDLE_TIMEOUT (3s) + SESSION_CHECK_INTERVAL (3.1s) so at least
        // one timeout check runs strictly after the idle timeout has elapsed
        Thread.sleep(8000L)
        assert(session.getNoOperationTime === 0L)
        assert(!session.isClosed)
        assert(sessionManager.getActiveUserSessionCount === 1)

        // Complete engine launch and verify openSession succeeds with an open session
        launchLatch.countDown()
        val handle = openFuture.get(5, TimeUnit.SECONDS)
        assert(handle === session.handle)
        assert(!session.isClosed)
        assert(session.launchEngineOp.isTerminal)
      }
    } finally {
      launchLatch.countDown()
      executor.shutdownNow()
    }
  }

  test("failed async engine launch session should be closed after idle timeout") {
    val launchLatch = new CountDownLatch(1)
    withTestSessionManager(launchLatch, failLaunch = true) { sessionManager =>
      val handle = sessionManager.openSession(
        TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
        "test-user",
        "anonymous",
        "127.0.0.1",
        Map(SESSION_ENGINE_LAUNCH_ASYNC.key -> "true"))

      val session = sessionManager.getSession(handle).asInstanceOf[KyuubiSessionImpl]
      launchLatch.countDown()

      eventually(timeout(5.seconds), interval(100.milliseconds)) {
        assert(session.launchEngineOp.isTerminal)
      }

      eventually(timeout(10.seconds), interval(200.milliseconds)) {
        assert(session.isClosed)
        assert(sessionManager.getActiveUserSessionCount === 0)
      }
    }
  }
}
