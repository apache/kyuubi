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

package org.apache.kyuubi.client

import java.net.{InetAddress, ServerSocket}
import java.util.concurrent.{Callable, CountDownLatch, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.concurrent.duration._

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doAnswer, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.{KyuubiOperation, OperationHandle, OperationState}
import org.apache.kyuubi.operation.FetchOrientation.FETCH_NEXT
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.service.authentication.PlainSASLHelper
import org.apache.kyuubi.session.{NoopSessionManager, Session, SessionHandle, SessionManager}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10
import org.apache.kyuubi.shaded.thrift.protocol.TBinaryProtocol
import org.apache.kyuubi.shaded.thrift.server.TThreadPoolServer
import org.apache.kyuubi.shaded.thrift.transport.TServerSocket
import org.apache.kyuubi.util.ThreadUtils

class KyuubiSyncThriftClientSuite extends KyuubiFunSuite {

  private val cleanupTimeoutKey = "kyuubi.session.engine.cleanup.timeout"

  private val cleanupCalls: Seq[(String, KyuubiSyncThriftClient => Unit)] = Seq(
    "CancelOperation" -> ((client: KyuubiSyncThriftClient) =>
      client.cancelOperation(remoteOperationHandle())),
    "CloseOperation" -> ((client: KyuubiSyncThriftClient) =>
      client.closeOperation(remoteOperationHandle())),
    "CloseSession" -> ((client: KyuubiSyncThriftClient) => client.closeSession()))

  cleanupCalls.foreach { case (name, cleanup) =>
    test(s"$name times out and closes the connection when the engine does not respond") {
      withEngine(name) { (client, engine) =>
        val caller = ThreadUtils.newDaemonSingleThreadScheduledExecutor("cleanup-caller")
        try {
          val result = caller.submit(new Callable[KyuubiSQLException] {
            override def call(): KyuubiSQLException = intercept[KyuubiSQLException] {
              cleanup(client)
            }
          })
          assert(engine.requestStarted.await(3, TimeUnit.SECONDS))
          val error = result.get(3, TimeUnit.SECONDS)
          assert(error.getMessage.contains(name))
          assert(error.getMessage.toLowerCase.contains("timed out"))
          assert(client.engineConnectionClosed)
          assert(client.asyncRequestInterrupted)
          assertRpcExecutorStopped(client)
          intercept[KyuubiSQLException] {
            client.getInfo(TGetInfoType.CLI_DBMS_VER)
          }
        } finally {
          engine.releaseResponse.countDown()
          ThreadUtils.shutdown(caller, 3.seconds)
        }
      }
    }
  }

  test("cleanup timeout includes waiting for a client lock held by another RPC") {
    withEngine("GetInfo") { (client, engine) =>
      val callers = ThreadUtils.newDaemonFixedThreadPool(2, "cleanup-lock-caller")
      try {
        val info = callers.submit(new Callable[Boolean] {
          override def call(): Boolean = {
            intercept[Exception] {
              client.getInfo(TGetInfoType.CLI_DBMS_VER)
            }
            true
          }
        })
        assert(engine.requestStarted.await(3, TimeUnit.SECONDS))
        val cancel = callers.submit(new Callable[KyuubiSQLException] {
          override def call(): KyuubiSQLException = intercept[KyuubiSQLException] {
            client.cancelOperation(remoteOperationHandle())
          }
        })
        assert(cancel.get(3, TimeUnit.SECONDS).getMessage.contains("CancelOperation"))
        assert(client.engineConnectionClosed)
        assert(info.get(3, TimeUnit.SECONDS))
        assertRpcExecutorStopped(client)
      } finally {
        engine.releaseResponse.countDown()
        ThreadUtils.shutdown(callers, 3.seconds)
      }
    }
  }

  test("successful cancellation leaves the session connection usable") {
    withEngine("") { (client, _) =>
      client.cancelOperation(remoteOperationHandle())
      assert(!client.engineConnectionClosed)
      assert(client.getInfo(TGetInfoType.CLI_DBMS_VER).getInfoValue.getStringValue === "test")
      client.closeOperation(remoteOperationHandle())
      assert(!client.engineConnectionClosed)
    }
  }

  test("lock acquisition and the RPC response share one cleanup timeout") {
    withEngine("GetInfo,CancelOperation", KyuubiConf().set(cleanupTimeoutKey, "PT3S")) {
      (client, engine) =>
        val callers = ThreadUtils.newDaemonFixedThreadPool(2, "cleanup-budget-caller")
        val cancelStarted = new CountDownLatch(1)
        try {
          val info = callers.submit(new Callable[String] {
            override def call(): String = {
              client.getInfo(TGetInfoType.CLI_DBMS_VER).getInfoValue.getStringValue
            }
          })
          assert(engine.requestStarted.await(3, TimeUnit.SECONDS))
          val cancel = callers.submit(new Callable[KyuubiSQLException] {
            override def call(): KyuubiSQLException = {
              cancelStarted.countDown()
              intercept[KyuubiSQLException] {
                client.cancelOperation(remoteOperationHandle())
              }
            }
          })
          assert(cancelStarted.await(3, TimeUnit.SECONDS))
          intercept[TimeoutException] {
            cancel.get(2, TimeUnit.SECONDS)
          }
          engine.releaseInfoResponse.countDown()
          assert(info.get(1, TimeUnit.SECONDS) === "test")
          assert(cancel.get(2, TimeUnit.SECONDS).getMessage.contains("CancelOperation"))
          assert(client.engineConnectionClosed)
          assertRpcExecutorStopped(client)
        } finally {
          engine.releaseInfoResponse.countDown()
          engine.releaseResponse.countDown()
          ThreadUtils.shutdown(callers, 3.seconds)
        }
    }
  }

  test("interrupting a cleanup waiter closes the connection and restores the interrupt") {
    withEngine("CancelOperation", KyuubiConf().set(cleanupTimeoutKey, "PT5S")) { (client, engine) =>
      val caller = ThreadUtils.newDaemonSingleThreadScheduledExecutor("interrupted-cleanup-caller")
      val requestThread = new AtomicReference[Thread]()
      try {
        val result = caller.submit(new Callable[Boolean] {
          override def call(): Boolean = {
            requestThread.set(Thread.currentThread())
            intercept[KyuubiSQLException] {
              client.cancelOperation(remoteOperationHandle())
            }
            Thread.currentThread().isInterrupted
          }
        })
        assert(engine.requestStarted.await(3, TimeUnit.SECONDS))
        requestThread.get().interrupt()
        assert(result.get(3, TimeUnit.SECONDS))
        assert(client.engineConnectionClosed)
        assertRpcExecutorStopped(client)
      } finally {
        engine.releaseResponse.countDown()
        ThreadUtils.shutdown(caller, 3.seconds)
      }
    }
  }

  test("ordinary RPCs are not limited by the cleanup timeout") {
    withEngine("GetInfo") { (client, engine) =>
      val caller = ThreadUtils.newDaemonSingleThreadScheduledExecutor("ordinary-rpc-caller")
      try {
        val info = caller.submit(new Callable[String] {
          override def call(): String = {
            client.getInfo(TGetInfoType.CLI_DBMS_VER).getInfoValue.getStringValue
          }
        })
        assert(engine.requestStarted.await(3, TimeUnit.SECONDS))
        intercept[TimeoutException] {
          info.get(400, TimeUnit.MILLISECONDS)
        }
        assert(!client.engineConnectionClosed)
        engine.releaseResponse.countDown()
        assert(info.get(3, TimeUnit.SECONDS) === "test")
      } finally {
        engine.releaseResponse.countDown()
        ThreadUtils.shutdown(caller, 3.seconds)
      }
    }
  }

  test("a responsive aliveness probe does not prevent cleanup RPC timeout") {
    val conf = KyuubiConf()
      .set(cleanupTimeoutKey, "PT1S")
      .set(KyuubiConf.ENGINE_ALIVE_PROBE_ENABLED, true)
      .set(KyuubiConf.ENGINE_ALIVE_PROBE_INTERVAL, 20L)
    withEngine("CancelOperation", conf) { (client, engine) =>
      val caller = ThreadUtils.newDaemonSingleThreadScheduledExecutor("probe-cleanup-caller")
      try {
        val result = caller.submit(new Callable[KyuubiSQLException] {
          override def call(): KyuubiSQLException = intercept[KyuubiSQLException] {
            client.cancelOperation(remoteOperationHandle())
          }
        })
        assert(engine.requestStarted.await(3, TimeUnit.SECONDS))
        eventually(timeout(800.millis)) {
          assert(engine.infoRequests.get() > 0)
        }
        assert(result.get(3, TimeUnit.SECONDS).getMessage.contains("CancelOperation"))
        assert(client.engineConnectionClosed)
      } finally {
        engine.releaseResponse.countDown()
        ThreadUtils.shutdown(caller, 3.seconds)
      }
    }
  }

  test("a stalled cancellation does not indefinitely block the session timeout-checker") {
    withEngine("CancelOperation", KyuubiConf().set(cleanupTimeoutKey, "PT5S")) { (client, engine) =>
      val logRoot = Utils.createTempDir()
      val conf = KyuubiConf()
        .set(KyuubiConf.SESSION_CHECK_INTERVAL, 4000L)
        .set(KyuubiConf.SERVER_OPERATION_LOG_DIR_ROOT, logRoot.toString)
      val manager = new TestSessionManager
      manager.initialize(conf)
      val stalledSession = mockSession(manager)
      val healthySession = mockSession(manager)
      val engineClient = client
      val operation = new KyuubiOperation(stalledSession) {
        override lazy val client: KyuubiSyncThriftClient = engineClient
        _remoteOpHandle = remoteOperationHandle()
        override protected def runInternal(): Unit = {}
      }
      val healthyChecks = new AtomicInteger()
      doAnswer { _: InvocationOnMock =>
        operation.close()
        null
      }.when(stalledSession).closeExpiredOperations()
      doAnswer { _: InvocationOnMock =>
        healthyChecks.incrementAndGet()
        null
      }.when(healthySession).closeExpiredOperations()
      manager.register(stalledSession)
      manager.register(healthySession)
      val caller = ThreadUtils.newDaemonSingleThreadScheduledExecutor("checker-cleanup-caller")
      try {
        val cancellation = caller.submit(new Callable[KyuubiSQLException] {
          override def call(): KyuubiSQLException = intercept[KyuubiSQLException] {
            operation.cancel()
          }
        })
        assert(engine.requestStarted.await(3, TimeUnit.SECONDS))
        manager.start()
        eventually(timeout(12.seconds)) {
          assert(healthyChecks.get() >= 2)
        }
        cancellation.get(3, TimeUnit.SECONDS)
        assert(client.engineConnectionClosed)
      } finally {
        engine.releaseResponse.countDown()
        ThreadUtils.shutdown(caller, 3.seconds)
        manager.stop()
        Utils.deleteDirectoryRecursively(logRoot.toFile)
      }
    }
  }

  test("closing an operation bounds waiting for its lock during a stalled result fetch") {
    withEngine("FetchResults") { (client, engine) =>
      val manager = mock[SessionManager]
      when(manager.getConf).thenReturn(KyuubiConf())
      val session = mockSession(manager)
      val engineClient = client
      val log = mock[OperationLog]
      val operation = new KyuubiOperation(session) {
        override lazy val client: KyuubiSyncThriftClient = engineClient
        override def getOperationLog: Option[OperationLog] = Some(log)
        _remoteOpHandle = remoteOperationHandle()
        setState(OperationState.RUNNING)
        setState(OperationState.FINISHED)
        override protected def runInternal(): Unit = {}
      }
      val callers = ThreadUtils.newDaemonFixedThreadPool(2, "operation-lock-caller")
      try {
        val fetching = callers.submit(new Callable[Exception] {
          override def call(): Exception = intercept[Exception] {
            operation.getNextRowSet(FETCH_NEXT, 1)
          }
        })
        assert(engine.requestStarted.await(3, TimeUnit.SECONDS))
        val closing = callers.submit(new Callable[KyuubiSQLException] {
          override def call(): KyuubiSQLException = intercept[KyuubiSQLException] {
            operation.close()
          }
        })
        assert(closing.get(3, TimeUnit.SECONDS).getMessage.contains("CloseOperation"))
        assert(client.engineConnectionClosed)
        fetching.get(3, TimeUnit.SECONDS)
        verify(log).close()
        assertRpcExecutorStopped(client)
      } finally {
        engine.releaseResponse.countDown()
        ThreadUtils.shutdown(callers, 3.seconds)
      }
    }
  }

  test("CloseSession bounds the companion probe session cleanup") {
    val conf = KyuubiConf()
      .set(cleanupTimeoutKey, "PT0.2S")
      .set(KyuubiConf.ENGINE_ALIVE_PROBE_ENABLED, true)
      .set(KyuubiConf.ENGINE_ALIVE_PROBE_INTERVAL, 1000L)
    withEngine("ProbeCloseSession", conf) { (client, engine) =>
      val caller = ThreadUtils.newDaemonSingleThreadScheduledExecutor("probe-close-caller")
      try {
        val result = caller.submit(new Callable[KyuubiSQLException] {
          override def call(): KyuubiSQLException = intercept[KyuubiSQLException] {
            client.closeSession()
          }
        })
        assert(engine.requestStarted.await(3, TimeUnit.SECONDS))
        assert(result.get(3, TimeUnit.SECONDS).getMessage.contains("CloseSession"))
        assert(client.engineConnectionClosed)
        assert(client.getEngineAliveProbeProtocol.forall(!_.getTransport.isOpen))
        assertRpcExecutorStopped(client)
      } finally {
        engine.releaseResponse.countDown()
        ThreadUtils.shutdown(caller, 3.seconds)
      }
    }
  }

  test("CloseSession bounds waiting for an in-flight aliveness probe to stop") {
    val conf = KyuubiConf()
      .set(cleanupTimeoutKey, "PT0.2S")
      .set(KyuubiConf.ENGINE_ALIVE_PROBE_ENABLED, true)
      .set(KyuubiConf.ENGINE_ALIVE_PROBE_INTERVAL, 1000L)
    withEngine("GetInfo", conf) { (client, engine) =>
      assert(engine.requestStarted.await(3, TimeUnit.SECONDS))
      val caller = ThreadUtils.newDaemonSingleThreadScheduledExecutor("probe-stop-caller")
      try {
        val result = caller.submit(new Callable[KyuubiSQLException] {
          override def call(): KyuubiSQLException = intercept[KyuubiSQLException] {
            client.closeSession()
          }
        })
        assert(result.get(3, TimeUnit.SECONDS).getMessage.contains("CloseSession"))
        assert(client.engineConnectionClosed)
        assert(client.getEngineAliveProbeProtocol.forall(!_.getTransport.isOpen))
        assertRpcExecutorStopped(client)
      } finally {
        engine.releaseResponse.countDown()
        ThreadUtils.shutdown(caller, 3.seconds)
      }
    }
  }

  private def remoteOperationHandle(): TOperationHandle = {
    val handle = OperationHandle().toTOperationHandle
    handle.setOperationType(TOperationType.EXECUTE_STATEMENT)
    handle
  }

  private def mockSession(manager: SessionManager): Session = {
    val session = mock[Session]
    when(session.handle).thenReturn(SessionHandle())
    when(session.protocol).thenReturn(HIVE_CLI_SERVICE_PROTOCOL_V10)
    when(session.user).thenReturn("test")
    when(session.sessionManager).thenReturn(manager)
    when(session.lastAccessTime).thenReturn(Long.MaxValue)
    session
  }

  private class TestSessionManager extends NoopSessionManager {
    def register(session: Session): Unit = setSession(session.handle, session)
  }

  private def assertRpcExecutorStopped(client: KyuubiSyncThriftClient): Unit = {
    eventually(timeout(3.seconds)) {
      assert(client.asyncRequestExecutorTerminated)
    }
  }

  private def withEngine(
      blockedMethod: String,
      conf: KyuubiConf = KyuubiConf().set(cleanupTimeoutKey, "PT0.2S"))(
      f: (KyuubiSyncThriftClient, TestEngine) => Unit): Unit = {
    val engine = new TestEngine(blockedMethod, conf)
    engine.start()
    var client: KyuubiSyncThriftClient = null
    try {
      client = KyuubiSyncThriftClient.createClient(
        "test",
        "anonymous",
        engine.host,
        engine.port,
        conf)
      client.openSession(HIVE_CLI_SERVICE_PROTOCOL_V10, "test", "anonymous", Map.empty)
      f(client, engine)
    } finally {
      engine.releaseInfoResponse.countDown()
      engine.releaseResponse.countDown()
      try {
        if (client != null && !client.engineConnectionClosed) client.closeSession()
      } finally {
        engine.stop()
      }
    }
  }

  private class TestEngine(blockedMethod: String, conf: KyuubiConf) {
    val requestStarted = new CountDownLatch(1)
    val releaseResponse = new CountDownLatch(1)
    val releaseInfoResponse = new CountDownLatch(1)
    val infoRequests = new AtomicInteger()
    private val openedSessions = new AtomicInteger()
    @volatile private var probeSessionHandle: TSessionHandle = _
    private val socket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
    val host: String = socket.getInetAddress.getHostAddress
    val port: Int = socket.getLocalPort
    private val handler = mock[TCLIService.Iface]
    private val workers = ThreadUtils.newDaemonCachedThreadPool("test-engine-worker")
    private val acceptor = ThreadUtils.newDaemonSingleThreadScheduledExecutor("test-engine")

    private def waitForResponse(method: String): Unit = {
      if (blockedMethod.split(",").contains(method)) {
        requestStarted.countDown()
        if (method == "GetInfo" && blockedMethod.contains(",")) {
          releaseInfoResponse.await()
        } else {
          releaseResponse.await()
        }
      }
    }

    private def success: TStatus = new TStatus(TStatusCode.SUCCESS_STATUS)

    when(handler.OpenSession(any[TOpenSessionReq]())).thenAnswer { _: InvocationOnMock =>
      val response = new TOpenSessionResp(success, HIVE_CLI_SERVICE_PROTOCOL_V10)
      response.setSessionHandle(SessionHandle().toTSessionHandle)
      if (openedSessions.incrementAndGet() > 1) {
        probeSessionHandle = response.getSessionHandle
      }
      response
    }
    when(handler.CancelOperation(any[TCancelOperationReq]())).thenAnswer { _: InvocationOnMock =>
      waitForResponse("CancelOperation")
      new TCancelOperationResp(success)
    }
    when(handler.CloseOperation(any[TCloseOperationReq]())).thenAnswer { _: InvocationOnMock =>
      waitForResponse("CloseOperation")
      new TCloseOperationResp(success)
    }
    when(handler.CloseSession(any[TCloseSessionReq]())).thenAnswer { invocation: InvocationOnMock =>
      val request = invocation.getArgument[TCloseSessionReq](0)
      val method = if (request.getSessionHandle == probeSessionHandle) {
        "ProbeCloseSession"
      } else {
        "CloseSession"
      }
      waitForResponse(method)
      new TCloseSessionResp(success)
    }
    when(handler.FetchResults(any[TFetchResultsReq]())).thenAnswer { _: InvocationOnMock =>
      waitForResponse("FetchResults")
      new TFetchResultsResp(success)
    }
    when(handler.GetInfo(any[TGetInfoReq]())).thenAnswer { _: InvocationOnMock =>
      infoRequests.incrementAndGet()
      waitForResponse("GetInfo")
      val value = new TGetInfoValue()
      value.setStringValue("test")
      new TGetInfoResp(success, value)
    }

    private val server = new TThreadPoolServer(new TThreadPoolServer.Args(new TServerSocket(socket))
      .processor(new TCLIService.Processor[TCLIService.Iface](handler))
      .transportFactory(PlainSASLHelper.getTransportFactory("NONE", conf))
      .protocolFactory(new TBinaryProtocol.Factory())
      .executorService(workers))

    def start(): Unit = {
      acceptor.submit(new Runnable {
        override def run(): Unit = server.serve()
      })
      eventually(timeout(3.seconds)) {
        assert(server.isServing)
      }
    }

    def stop(): Unit = {
      releaseInfoResponse.countDown()
      releaseResponse.countDown()
      server.stop()
      workers.shutdownNow()
      ThreadUtils.shutdown(acceptor, 3.seconds)
      ThreadUtils.shutdown(workers, 3.seconds)
    }
  }
}
