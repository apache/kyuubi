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

package org.apache.kyuubi.server.api.v1

import java.io.{ByteArrayOutputStream, OutputStream}
import java.util.UUID
import javax.servlet.{ServletOutputStream, WriteListener}
import javax.servlet.http.HttpServletResponse
import javax.ws.rs.client.Entity
import javax.ws.rs.core.MediaType

import org.mockito.Mockito.{mock, verify, when}

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper}
import org.apache.kyuubi.client.api.v1.dto.{ApprovalRequest, ChatRequest}
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ENGINE_REF_ID, HA_NAMESPACE}
import org.apache.kyuubi.ha.client.{DataAgentSessionRoute, DiscoveryClientProvider}

class DataAgentResourceSuite extends KyuubiFunSuite with RestFrontendTestHelper {

  // -- chat preflight ---------------------------------------------------------

  test("chat returns 400 for malformed sessionHandle") {
    val response = webTarget.path("api/v1/data-agent/not-a-uuid/chat")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(new ChatRequest("hi"), MediaType.APPLICATION_JSON_TYPE))
    assert(response.getStatus === 400)
  }

  test("chat returns 404 for unknown sessionHandle") {
    val unknown = UUID.randomUUID().toString
    val response = webTarget.path(s"api/v1/data-agent/$unknown/chat")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(new ChatRequest("hi"), MediaType.APPLICATION_JSON_TYPE))
    assert(response.getStatus === 404)
  }

  test("get and delete return 400 for malformed sessionHandle") {
    val getResponse = webTarget.path("api/v1/data-agent/sessions/not-a-uuid")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()
    assert(getResponse.getStatus === 400)

    val deleteResponse = webTarget.path("api/v1/data-agent/sessions/not-a-uuid")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .delete()
    assert(deleteResponse.getStatus === 400)
  }

  test("get and delete return 404 for unknown sessionHandle") {
    val unknown = UUID.randomUUID().toString
    val getResponse = webTarget.path(s"api/v1/data-agent/sessions/$unknown")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()
    assert(getResponse.getStatus === 404)

    val deleteResponse = webTarget.path(s"api/v1/data-agent/sessions/$unknown")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .delete()
    assert(deleteResponse.getStatus === 404)
  }

  test("stale route returns 410 and is removed") {
    val sessionId = UUID.randomUUID().toString
    registerRoute(sessionId, "anonymous")

    val response = webTarget.path(s"api/v1/data-agent/sessions/$sessionId")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()
    assert(response.getStatus === 410)
    assert(findRoute(sessionId).isEmpty)

    val secondResponse = webTarget.path(s"api/v1/data-agent/sessions/$sessionId")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()
    assert(secondResponse.getStatus === 404)
  }

  test("delete removes a stale route and returns 410") {
    val sessionId = UUID.randomUUID().toString
    registerRoute(sessionId, "anonymous")

    val response = webTarget.path(s"api/v1/data-agent/sessions/$sessionId")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .delete()
    assert(response.getStatus === 410)
    assert(findRoute(sessionId).isEmpty)
  }

  test("chat removes a stale route before opening the SSE stream") {
    val sessionId = UUID.randomUUID().toString
    registerRoute(sessionId, "anonymous")

    val response = webTarget.path(s"api/v1/data-agent/$sessionId/chat")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(new ChatRequest("hi"), MediaType.APPLICATION_JSON_TYPE))
    assert(response.getStatus === 410)
    assert(findRoute(sessionId).isEmpty)
  }

  test("approve removes a stale route and returns 410") {
    val sessionId = UUID.randomUUID().toString
    registerRoute(sessionId, "anonymous")
    val request = new ApprovalRequest("request-id", true)

    val response = webTarget.path(s"api/v1/data-agent/$sessionId/approve")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))
    assert(response.getStatus === 410)
    assert(findRoute(sessionId).isEmpty)
  }

  test("connection refusal removes a stale route and returns 410") {
    val sessionId = UUID.randomUUID().toString
    val engineSpace = s"/unreachable-engine-$sessionId"
    val engineRefId = s"unreachable-ref-$sessionId"
    val engineConf = conf.clone.set(HA_ENGINE_REF_ID, engineRefId)
    val servicePath = DiscoveryClientProvider.withDiscoveryClient(conf) { discovery =>
      val path = discovery.createAndGetServiceNode(
        engineConf,
        engineSpace,
        "127.0.0.1:1",
        external = true)
      DataAgentSessionRoute.register(
        discovery,
        conf.get(HA_NAMESPACE),
        sessionId,
        DataAgentSessionRoute(engineSpace, engineRefId, "anonymous"))
      path
    }

    try {
      val response = webTarget.path(s"api/v1/data-agent/sessions/$sessionId")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .get()
      assert(response.getStatus === 410)
      assert(findRoute(sessionId).isEmpty)

      val secondResponse = webTarget.path(s"api/v1/data-agent/sessions/$sessionId")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .get()
      assert(secondResponse.getStatus === 404)
    } finally {
      DiscoveryClientProvider.withDiscoveryClient(conf) { discovery =>
        if (discovery.pathExists(servicePath)) discovery.delete(servicePath)
      }
    }
  }

  // -- approve preflight ------------------------------------------------------

  test("approve returns 400 for null body") {
    val unknown = UUID.randomUUID().toString
    val response = webTarget.path(s"api/v1/data-agent/$unknown/approve")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(""))
    assert(response.getStatus === 400)
  }

  test("approve returns 400 for empty/whitespace requestId") {
    val unknown = UUID.randomUUID().toString
    Seq("", "   ").foreach { id =>
      val req = new ApprovalRequest()
      req.setRequestId(id)
      req.setApproved(true)
      val response = webTarget.path(s"api/v1/data-agent/$unknown/approve")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE))
      assert(response.getStatus === 400, s"expected 400 for requestId=[$id]")
    }
  }

  test("approve returns 400 for requestId with disallowed chars") {
    // The id is concatenated into `__approve:<id>` so anything that could break command
    // parsing or arrive with newlines must be rejected.
    val bad = Seq(
      "abc\nxyz", // newline -- log/command injection
      "abc xyz", // space
      "abc/xyz", // slash
      "abc;xyz", // semicolon
      "abc'xyz", // quote
      "<script>",
      "../etc/passwd")
    val unknown = UUID.randomUUID().toString
    bad.foreach { id =>
      val req = new ApprovalRequest()
      req.setRequestId(id)
      req.setApproved(true)
      val response = webTarget.path(s"api/v1/data-agent/$unknown/approve")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE))
      assert(response.getStatus === 400, s"expected 400 for requestId=[$id]")
    }
  }

  test("approve returns 400 for over-length requestId") {
    val req = new ApprovalRequest()
    req.setRequestId("a" * 257) // MAX_REQUEST_ID_LENGTH = 256
    req.setApproved(true)
    val unknown = UUID.randomUUID().toString
    val response = webTarget.path(s"api/v1/data-agent/$unknown/approve")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE))
    assert(response.getStatus === 400)
  }

  test("approve accepts well-formed requestId charset (validation passes session lookup)") {
    // With a well-formed id but unknown session, validation should pass and we should land
    // on the 404 path -- proving the id charset is accepted.
    val unknown = UUID.randomUUID().toString
    Seq("abc-123", "req_42", "task.5", "ns:id", "user@host", "v1.2.3-rc4").foreach { id =>
      val req = new ApprovalRequest()
      req.setRequestId(id)
      req.setApproved(true)
      val response = webTarget.path(s"api/v1/data-agent/$unknown/approve")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE))
      assert(response.getStatus === 404, s"expected 404 (not 400) for valid id=[$id]")
    }
  }

  // -- SSE framing ------------------------------------------------------------

  import DataAgentResource._

  test("SseStream.event splits multi-line JSON into separate data: lines") {
    val (stream, sink) = newStreamWithSink()
    stream.open()
    stream.event("message", "{\n  \"a\": 1\n}")
    val out = sink.toString("UTF-8")
    val frame = out.substring(out.indexOf("event:")) // strip pre-event headers if any
    assert(frame.startsWith("event: message\n"))
    assert(frame.contains("data: {\n"))
    assert(frame.contains("data:   \"a\": 1\n"))
    assert(frame.contains("data: }\n"))
    assert(frame.endsWith("\n\n"), s"frame must end with blank line, got: ${frame.takeRight(8)}")
  }

  test("SseStream.event preserves single-line JSON unchanged") {
    val (stream, sink) = newStreamWithSink()
    stream.open()
    stream.event("content_delta", """{"text":"hello"}""")
    val out = sink.toString("UTF-8")
    assert(out.contains("event: content_delta\n"))
    assert(out.contains("data: {\"text\":\"hello\"}\n\n"))
  }

  test("SseStream.keepalive emits named ping event (not a comment frame)") {
    val (stream, sink) = newStreamWithSink()
    stream.open()
    stream.keepalive()
    val out = sink.toString("UTF-8")
    // Named events reach EventSource dispatchers; comment frames (starting `:`) do not.
    assert(out.contains("event: ping\ndata: {}\n\n"))
    assert(!out.contains(": ka"))
  }

  test("SseStream.open sets the headers that matter for streaming through proxies") {
    val response = mock(classOf[HttpServletResponse])
    val sink = new ByteArrayOutputStream()
    when(response.getOutputStream).thenReturn(servletStreamOver(sink))
    new SseStream(response).open()
    verify(response).setContentType("text/event-stream")
    // Without this nginx buffers SSE frames and the client only sees them on connection close.
    verify(response).setHeader("X-Accel-Buffering", "no")
  }

  // -- helpers ----------------------------------------------------------------

  private def newStreamWithSink(): (SseStream, ByteArrayOutputStream) = {
    val response = mock(classOf[HttpServletResponse])
    val sink = new ByteArrayOutputStream()
    when(response.getOutputStream).thenReturn(servletStreamOver(sink))
    (new SseStream(response), sink)
  }

  private def registerRoute(sessionId: String, user: String): DataAgentSessionRoute = {
    val route = DataAgentSessionRoute("missing-engine-space", "missing-engine-ref", user)
    DiscoveryClientProvider.withDiscoveryClient(conf) { discovery =>
      DataAgentSessionRoute.register(discovery, conf.get(HA_NAMESPACE), sessionId, route)
    }
    route
  }

  private def findRoute(sessionId: String): Option[DataAgentSessionRoute] = {
    DiscoveryClientProvider.withDiscoveryClient(conf) { discovery =>
      DataAgentSessionRoute.find(discovery, conf.get(HA_NAMESPACE), sessionId)
    }
  }

  private def servletStreamOver(sink: OutputStream): ServletOutputStream =
    new ServletOutputStream {
      override def write(b: Int): Unit = sink.write(b)
      override def write(b: Array[Byte], off: Int, len: Int): Unit = sink.write(b, off, len)
      override def isReady: Boolean = true
      override def setWriteListener(writeListener: WriteListener): Unit = ()
    }
}
