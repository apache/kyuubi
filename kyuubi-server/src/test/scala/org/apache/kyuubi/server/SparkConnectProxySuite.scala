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

import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.collection.JavaConverters._

import io.grpc._
import io.grpc.MethodDescriptor.MethodType
import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.client.KyuubiGrpcClient
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.service.{AbstractBackendService, AbstractFrontendService, AbstractGrpcFrontendService, GrpcProxyServerCallHandler, Serverable, Service}
import org.apache.kyuubi.service.GrpcProxyServerCallHandler.byteMarshaller
import org.apache.kyuubi.session.{GrpcSessionHandle, KyuubiGrpcSession, KyuubiGrpcSessionManager, SessionHandle}
import org.apache.kyuubi.shaded.spark.connect.proto

/**
 * Verifies the full proxy of the current Spark Connect proto against a mock
 * engine, including all typed RPCs and the byte-level passthrough of unknown
 * service methods.
 */
class SparkConnectProxySuite extends KyuubiFunSuite {

  private var mockServer: Server = _
  private var frontend: AbstractGrpcFrontendService = _
  private var clientChannel: ManagedChannel = _

  override def afterAll(): Unit = {
    Option(clientChannel).foreach { channel =>
      channel.shutdownNow()
      channel.awaitTermination(10, TimeUnit.SECONDS)
    }
    Option(frontend).foreach(_.stop())
    Option(mockServer).foreach { server =>
      server.shutdownNow()
      server.awaitTermination(10, TimeUnit.SECONDS)
    }
    super.afterAll()
  }

  test("proxy every RPC of the current Spark Connect proto") {
    val mockEngine = new MockEngine()
    val testSessionManager = new TestSessionManager()
    startServices(mockEngine, testSessionManager)

    val sessionId = UUID.randomUUID.toString
    val userId = "alice"
    val userContext = () => proto.UserContext.newBuilder.setUserId(userId).build()
    val bstub = proto.SparkConnectServiceGrpc.newBlockingStub(clientChannel)
    val astub = proto.SparkConnectServiceGrpc.newStub(clientChannel)

    // Unary RPC, also establishes the connection-scoped session.
    val configResp = bstub.config(proto.ConfigRequest.newBuilder
      .setUserContext(userContext())
      .setSessionId(sessionId)
      .build())
    assert(configResp.getSessionId == sessionId)

    // Server-streaming RPC.
    val executeResponses = new ConcurrentLinkedQueue[proto.ExecutePlanResponse]()
    val executeLatch = new CountDownLatch(1)
    astub.executePlan(
      proto.ExecutePlanRequest.newBuilder
        .setUserContext(userContext())
        .setSessionId(sessionId)
        .build(),
      new StreamObserver[proto.ExecutePlanResponse] {
        override def onNext(resp: proto.ExecutePlanResponse): Unit = executeResponses.add(resp)
        override def onError(t: Throwable): Unit = executeLatch.countDown()
        override def onCompleted(): Unit = executeLatch.countDown()
      })
    assert(executeLatch.await(10, TimeUnit.SECONDS))
    assert(executeResponses.asScala.toSeq.map(_.getSessionId) == Seq(sessionId))

    // Unary RPC.
    val analyzeResp = bstub.analyzePlan(proto.AnalyzePlanRequest.newBuilder
      .setUserContext(userContext())
      .setSessionId(sessionId)
      .build())
    assert(analyzeResp.getSessionId == sessionId)

    // Client-streaming RPC.
    val artifactLatch = new CountDownLatch(1)
    val artifactResponses = new ConcurrentLinkedQueue[proto.AddArtifactsResponse]()
    val requestObserver = astub.addArtifacts(
      new StreamObserver[proto.AddArtifactsResponse] {
        override def onNext(resp: proto.AddArtifactsResponse): Unit = artifactResponses.add(resp)
        override def onError(t: Throwable): Unit = artifactLatch.countDown()
        override def onCompleted(): Unit = artifactLatch.countDown()
      })
    requestObserver.onNext(proto.AddArtifactsRequest.newBuilder
      .setUserContext(userContext())
      .setSessionId(sessionId)
      .setBeginChunk(proto.AddArtifactsRequest.BeginChunkedArtifact.newBuilder.setName("f.py"))
      .build())
    requestObserver.onNext(proto.AddArtifactsRequest.newBuilder
      .setUserContext(userContext())
      .setSessionId(sessionId)
      .setChunk(proto.AddArtifactsRequest.ArtifactChunk.newBuilder.setData(
        com.google.protobuf.ByteString.copyFromUtf8("print(1)")))
      .build())
    requestObserver.onCompleted()
    assert(artifactLatch.await(10, TimeUnit.SECONDS))
    assert(artifactResponses.asScala.toSeq.map(_.getSessionId) == Seq(sessionId))

    // Unary RPCs.
    val artifactStatusResp = bstub.artifactStatus(proto.ArtifactStatusesRequest.newBuilder
      .setUserContext(userContext())
      .setSessionId(sessionId)
      .addNames("f.py")
      .build())
    assert(artifactStatusResp.getSessionId == sessionId)

    val interruptResp = bstub.interrupt(proto.InterruptRequest.newBuilder
      .setUserContext(userContext())
      .setSessionId(sessionId)
      .setInterruptType(proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_ALL)
      .build())
    assert(interruptResp.getSessionId == sessionId)

    // Server-streaming RPC.
    val reattachResponses = new ConcurrentLinkedQueue[proto.ExecutePlanResponse]()
    val reattachLatch = new CountDownLatch(1)
    astub.reattachExecute(
      proto.ReattachExecuteRequest.newBuilder
        .setUserContext(userContext())
        .setSessionId(sessionId)
        .setOperationId("op-1")
        .build(),
      new StreamObserver[proto.ExecutePlanResponse] {
        override def onNext(resp: proto.ExecutePlanResponse): Unit = reattachResponses.add(resp)
        override def onError(t: Throwable): Unit = reattachLatch.countDown()
        override def onCompleted(): Unit = reattachLatch.countDown()
      })
    assert(reattachLatch.await(10, TimeUnit.SECONDS))
    assert(reattachResponses.asScala.toSeq.map(_.getSessionId) == Seq(sessionId))

    val releaseExecuteResp = bstub.releaseExecute(proto.ReleaseExecuteRequest.newBuilder
      .setUserContext(userContext())
      .setSessionId(sessionId)
      .setOperationId("op-1")
      .build())
    assert(releaseExecuteResp.getSessionId == sessionId)

    val fetchErrorDetailsResp = bstub.fetchErrorDetails(proto.FetchErrorDetailsRequest.newBuilder
      .setUserContext(userContext())
      .setSessionId(sessionId)
      .setErrorId("error-1")
      .build())
    assert(fetchErrorDetailsResp.getSessionId == sessionId)

    // RPC of an unknown service is passed through as raw bytes on the connection.
    val (status, echoes) = invokeUnknown("ext.Plugin/Custom", "hello")
    assert(status.isOk && echoes == Seq("hello"))

    // ReleaseSession closes the Kyuubi session after the engine acknowledges it.
    val releaseSessionResp = bstub.releaseSession(proto.ReleaseSessionRequest.newBuilder
      .setUserContext(userContext())
      .setSessionId(sessionId)
      .build())
    assert(releaseSessionResp.getSessionId == sessionId)
    eventually {
      assert(testSessionManager.closedSessionIds.contains(sessionId))
    }

    val expectedCalls = Seq(
      "config",
      "executePlan",
      "analyzePlan",
      "addArtifacts",
      "artifactStatus",
      "interrupt",
      "reattachExecute",
      "releaseExecute",
      "fetchErrorDetails",
      "releaseSession")
    assert(mockEngine.calls.asScala.toSeq == expectedCalls)
    assert(mockEngine.sessionIds.asScala.toSeq.forall(_ == sessionId))
    // The user identity is normalized by the proxy.
    assert(mockEngine.userNames.asScala.toSeq.forall(_ == userId))
  }

  test("unknown RPC without an established session is rejected") {
    val mockEngine = new MockEngine()
    val testSessionManager = new TestSessionManager()
    startServices(mockEngine, testSessionManager)

    val (status, echoes) = invokeUnknown("ext.Plugin/Custom", "hello")
    assert(status.getCode == Status.Code.UNIMPLEMENTED && echoes.isEmpty)
  }

  private def startServices(mockEngine: MockEngine, manager: KyuubiGrpcSessionManager): Unit = {
    mockServer = NettyServerBuilder.forPort(0)
      .addService(proto.SparkConnectServiceGrpc.bindService(mockEngine))
      .fallbackHandlerRegistry(
        new GrpcProxyServerCallHandler.PassthroughHandlerRegistry(echoHandler))
      .build()
      .start()

    val backend = new KyuubiGrpcBackendService {
      override val sessionManager: KyuubiGrpcSessionManager = manager
    }

    frontend = new TestFrontend(backend)
    val conf = KyuubiConf()
    conf.set(FRONTEND_GRPC_BIND_HOST, Some("127.0.0.1"))
    conf.set(FRONTEND_GRPC_BIND_PORT, 0)
    backend.initialize(conf)
    frontend.initialize(conf)
    frontend.start()
    eventually {
      assert(frontend.asInstanceOf[TestFrontend].boundPort > 0)
    }
    clientChannel = ManagedChannelBuilder
      .forAddress("localhost", frontend.asInstanceOf[TestFrontend].boundPort)
      .usePlaintext()
      .build()
  }

  private class TestFrontend(backend: KyuubiGrpcBackendService)
    extends AbstractGrpcFrontendService("test-grpc-frontend") {

    override val serverable: Serverable = new Serverable("dummy") {
      override val backendService: AbstractBackendService = null
      override val frontendServices: Seq[AbstractFrontendService] = Seq.empty
      override protected def stopServer(): Unit = {}
    }

    override val discoveryService: Option[Service] = None

    override def sparkConnectService: BindableService = backend

    override def fallbackHandler: ServerCallHandler[Array[Byte], Array[Byte]] =
      new KyuubiGrpcProxyHandler(backend.sessionManager)

    def boundPort: Int = server.getPort
  }

  private def invokeUnknown(methodName: String, payload: String): (Status, Seq[String]) = {
    val methodDescriptor = MethodDescriptor.newBuilder(byteMarshaller, byteMarshaller)
      .setFullMethodName(methodName)
      .setType(MethodType.SERVER_STREAMING)
      .build()
    val call = clientChannel.newCall(methodDescriptor, CallOptions.DEFAULT)
    val latch = new CountDownLatch(1)
    val received = new ConcurrentLinkedQueue[String]()
    var status: Status = null
    call.start(
      new ClientCall.Listener[Array[Byte]] {
        override def onMessage(message: Array[Byte]): Unit =
          received.add(new String(message, UTF_8))
        override def onClose(st: Status, trailers: Metadata): Unit = {
          status = st
          latch.countDown()
        }
      },
      new Metadata())
    call.request(100)
    call.sendMessage(payload.getBytes(UTF_8))
    call.halfClose()
    assert(latch.await(10, TimeUnit.SECONDS))
    (status, received.asScala.toSeq)
  }

  private val echoHandler = new ServerCallHandler[Array[Byte], Array[Byte]] {
    override def startCall(
        call: ServerCall[Array[Byte], Array[Byte]],
        headers: Metadata): ServerCall.Listener[Array[Byte]] = {
      new ServerCall.Listener[Array[Byte]] {
        private val received = new java.util.ArrayList[Array[Byte]]()
        call.request(1)

        override def onMessage(message: Array[Byte]): Unit = {
          received.add(message)
          call.request(1)
        }

        override def onHalfClose(): Unit = {
          call.sendHeaders(new Metadata())
          received.asScala.foreach(call.sendMessage)
          call.close(Status.OK, new Metadata())
        }
      }
    }
  }

  private class TestSession(
      sessionKey: GrpcSessionHandle,
      conf: Map[String, String],
      sessionConf: KyuubiConf,
      sessionManager: KyuubiGrpcSessionManager)
    extends KyuubiGrpcSession(sessionKey, conf, sessionConf, sessionManager) {

    override def open(): Unit = {
      val channel = ManagedChannelBuilder
        .forAddress("localhost", mockServer.getPort)
        .usePlaintext()
        .build()
      _client = new KyuubiGrpcClient(
        KyuubiGrpcClient.Configuration(
          userId = sessionKey.userId,
          userName = sessionKey.userId,
          sessionId = Some(sessionKey.sessionId)),
        channel)
    }
  }

  private class TestSessionManager extends KyuubiGrpcSessionManager {
    val closedSessionIds = new ConcurrentLinkedQueue[String]()
    private val sessions = new ConcurrentHashMap[GrpcSessionHandle, TestSession]()

    override def getOrCreateSession(
        sessionKey: GrpcSessionHandle,
        previouslyObservedSesssionId: Option[String]): KyuubiGrpcSession = {
      var session = sessions.get(sessionKey)
      if (session == null) {
        session = new TestSession(sessionKey, Map.empty, conf.clone, this)
        val existing = sessions.putIfAbsent(sessionKey, session)
        if (existing == null) {
          setSession(session.handle, session)
          session.open()
        } else {
          session = existing
        }
      }
      session
    }

    override def closeSession(handle: SessionHandle): Unit = {
      closedSessionIds.add(handle.identifier.toString)
      sessions.remove(handle)
      super.closeSession(handle)
    }
  }

  private class MockEngine extends proto.SparkConnectServiceGrpc.AsyncService {
    val calls = new ConcurrentLinkedQueue[String]()
    val sessionIds = new ConcurrentLinkedQueue[String]()
    val userNames = new ConcurrentLinkedQueue[String]()

    private def record(name: String, userContext: proto.UserContext, sessionId: String): Unit = {
      calls.add(name)
      sessionIds.add(sessionId)
      userNames.add(userContext.getUserName)
    }

    override def executePlan(
        req: proto.ExecutePlanRequest,
        respObserver: StreamObserver[proto.ExecutePlanResponse]): Unit = {
      record("executePlan", req.getUserContext, req.getSessionId)
      respObserver.onNext(proto.ExecutePlanResponse.newBuilder
        .setSessionId(req.getSessionId)
        .setServerSideSessionId("engine-session")
        .build())
      respObserver.onCompleted()
    }

    override def analyzePlan(
        req: proto.AnalyzePlanRequest,
        respObserver: StreamObserver[proto.AnalyzePlanResponse]): Unit = {
      record("analyzePlan", req.getUserContext, req.getSessionId)
      respObserver.onNext(proto.AnalyzePlanResponse.newBuilder
        .setSessionId(req.getSessionId)
        .setServerSideSessionId("engine-session")
        .build())
      respObserver.onCompleted()
    }

    override def config(
        req: proto.ConfigRequest,
        respObserver: StreamObserver[proto.ConfigResponse]): Unit = {
      record("config", req.getUserContext, req.getSessionId)
      respObserver.onNext(proto.ConfigResponse.newBuilder
        .setSessionId(req.getSessionId)
        .setServerSideSessionId("engine-session")
        .build())
      respObserver.onCompleted()
    }

    override def addArtifacts(respObserver: StreamObserver[proto.AddArtifactsResponse])
        : StreamObserver[proto.AddArtifactsRequest] = {
      new StreamObserver[proto.AddArtifactsRequest] {
        private var sessionId: String = _

        override def onNext(req: proto.AddArtifactsRequest): Unit = {
          if (sessionId == null) {
            record("addArtifacts", req.getUserContext, req.getSessionId)
          }
          sessionId = req.getSessionId
        }

        override def onError(t: Throwable): Unit = respObserver.onError(t)

        override def onCompleted(): Unit = {
          respObserver.onNext(proto.AddArtifactsResponse.newBuilder
            .setSessionId(sessionId)
            .setServerSideSessionId("engine-session")
            .build())
          respObserver.onCompleted()
        }
      }
    }

    override def artifactStatus(
        req: proto.ArtifactStatusesRequest,
        respObserver: StreamObserver[proto.ArtifactStatusesResponse]): Unit = {
      record("artifactStatus", req.getUserContext, req.getSessionId)
      respObserver.onNext(proto.ArtifactStatusesResponse.newBuilder
        .setSessionId(req.getSessionId)
        .setServerSideSessionId("engine-session")
        .build())
      respObserver.onCompleted()
    }

    override def interrupt(
        req: proto.InterruptRequest,
        respObserver: StreamObserver[proto.InterruptResponse]): Unit = {
      record("interrupt", req.getUserContext, req.getSessionId)
      respObserver.onNext(proto.InterruptResponse.newBuilder
        .setSessionId(req.getSessionId)
        .setServerSideSessionId("engine-session")
        .build())
      respObserver.onCompleted()
    }

    override def reattachExecute(
        req: proto.ReattachExecuteRequest,
        respObserver: StreamObserver[proto.ExecutePlanResponse]): Unit = {
      record("reattachExecute", req.getUserContext, req.getSessionId)
      respObserver.onNext(proto.ExecutePlanResponse.newBuilder
        .setSessionId(req.getSessionId)
        .setServerSideSessionId("engine-session")
        .build())
      respObserver.onCompleted()
    }

    override def releaseExecute(
        req: proto.ReleaseExecuteRequest,
        respObserver: StreamObserver[proto.ReleaseExecuteResponse]): Unit = {
      record("releaseExecute", req.getUserContext, req.getSessionId)
      respObserver.onNext(proto.ReleaseExecuteResponse.newBuilder
        .setSessionId(req.getSessionId)
        .setServerSideSessionId("engine-session")
        .build())
      respObserver.onCompleted()
    }

    override def releaseSession(
        req: proto.ReleaseSessionRequest,
        respObserver: StreamObserver[proto.ReleaseSessionResponse]): Unit = {
      record("releaseSession", req.getUserContext, req.getSessionId)
      respObserver.onNext(proto.ReleaseSessionResponse.newBuilder
        .setSessionId(req.getSessionId)
        .setServerSideSessionId("engine-session")
        .build())
      respObserver.onCompleted()
    }

    override def fetchErrorDetails(
        req: proto.FetchErrorDetailsRequest,
        respObserver: StreamObserver[proto.FetchErrorDetailsResponse]): Unit = {
      record("fetchErrorDetails", req.getUserContext, req.getSessionId)
      respObserver.onNext(proto.FetchErrorDetailsResponse.newBuilder
        .setSessionId(req.getSessionId)
        .setServerSideSessionId("engine-session")
        .build())
      respObserver.onCompleted()
    }
  }
}
