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

import java.io.{IOException, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.util.concurrent.{CompletableFuture, ExecutionException, ExecutorService, RejectedExecutionException, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.regex.Pattern
import javax.servlet.http.HttpServletResponse
import javax.ws.rs._
import javax.ws.rs.core.{Context, MediaType}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.ObjectMapper
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.client.KyuubiSyncThriftClient
import org.apache.kyuubi.client.api.v1.dto.{ApprovalRequest, ChatRequest}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_SECURITY_ENABLED
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_NAMESPACE
import org.apache.kyuubi.ha.client.{DataAgentSessionRoute, DiscoveryClientProvider}
import org.apache.kyuubi.ha.client.ServiceDiscovery
import org.apache.kyuubi.operation.FetchOrientation
import org.apache.kyuubi.server.api.ApiRequestContext
import org.apache.kyuubi.service.authentication.InternalSecurityAccessor
import org.apache.kyuubi.session.{KyuubiSessionImpl, SessionHandle}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.util.ThreadUtils

@Tag(name = "DataAgent")
@Consumes(Array(MediaType.APPLICATION_JSON))
private[v1] class DataAgentResource extends ApiRequestContext with Logging {

  import DataAgentResource._

  private def verifySessionOwnership(session: KyuubiSessionImpl, userName: String): Unit = {
    if (!fe.isAdministrator(userName) && session.user != userName) {
      throw new ForbiddenException(
        s"$userName is not allowed to access session ${session.handle}")
    }
  }

  // Keep auth failures as 4xx responses before any SSE bytes are sent.
  private def parseSessionHandle(sessionHandleStr: String): SessionHandle = {
    val sessionHandle =
      try {
        SessionHandle.fromUUID(sessionHandleStr)
      } catch {
        case _: IllegalArgumentException =>
          throw new WebApplicationException("invalid sessionHandle", 400)
      }
    sessionHandle
  }

  private case class ResolvedClient(
      client: KyuubiSyncThriftClient,
      session: Option[KyuubiSessionImpl],
      closeTransport: () => Unit)

  private def routeExists(sessionHandleStr: String): Boolean =
    DiscoveryClientProvider.withDiscoveryClient(fe.getConf) { discovery =>
      DataAgentSessionRoute.find(
        discovery,
        fe.getConf.get(HA_NAMESPACE),
        sessionHandleStr).isDefined
    }

  private def resolveClient(sessionHandleStr: String): ResolvedClient = {
    val sessionHandle = parseSessionHandle(sessionHandleStr)
    val userName = fe.getSessionUser(Map.empty[String, String])
    fe.be.sessionManager.getSessionOption(sessionHandle) match {
      case Some(session: KyuubiSessionImpl) =>
        verifySessionOwnership(session, userName)
        val timeout = fe.getConf.get(KyuubiConf.FRONTEND_DATA_AGENT_OPERATION_TIMEOUT)
        session.launchEngineOp.getBackgroundHandle.get(timeout, TimeUnit.MILLISECONDS)
        if (session.client == null) {
          throw new WebApplicationException("Engine session is not ready", 503)
        }
        if (ServiceDiscovery.supportServiceDiscovery(fe.getConf) &&
          !routeExists(sessionHandleStr)) {
          try fe.be.closeSession(session.handle)
          catch {
            case NonFatal(e) => warn(s"Failed to clean up expired session ${session.handle}", e)
          }
          throw new WebApplicationException("session expired", 410)
        }
        ResolvedClient(session.client, Some(session), () => ())
      case _ =>
        if (!ServiceDiscovery.supportServiceDiscovery(fe.getConf)) {
          throw new WebApplicationException("session not found", 404)
        }
        val route = DiscoveryClientProvider.withDiscoveryClient(fe.getConf) { discovery =>
          val serverSpace = fe.getConf.get(HA_NAMESPACE)
          val found = DataAgentSessionRoute.find(discovery, serverSpace, sessionHandleStr)
            .getOrElse(throw new WebApplicationException("session not found", 404))
          if (!fe.isAdministrator(userName) && found.user != userName) {
            throw new WebApplicationException("session not found", 404)
          }
          val hostPort = discovery.getEngineByRefId(found.engineSpace, found.engineRefId)
            .getOrElse {
              DataAgentSessionRoute.unregister(discovery, serverSpace, sessionHandleStr)
              throw new WebApplicationException("session expired", 410)
            }
          (found, hostPort)
        }
        val password = if (fe.getConf.get(ENGINE_SECURITY_ENABLED)) {
          InternalSecurityAccessor.get().issueToken()
        } else {
          "anonymous"
        }
        val client = KyuubiSyncThriftClient.createAttachedClient(
          route._1.user,
          password,
          route._2._1,
          route._2._2,
          fe.getConf,
          sessionHandle)
        ResolvedClient(client, None, () => client.closeTransport())
    }
  }

  @GET
  @Path("sessions/{sessionHandle}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getSession(@PathParam("sessionHandle") sessionHandleStr: String): String = {
    val resolved = resolveClient(sessionHandleStr)
    try {
      resolved.client.getInfo(TGetInfoType.CLI_DBMS_VER)
      "{\"status\":\"ok\"}"
    } finally resolved.closeTransport()
  }

  @DELETE
  @Path("sessions/{sessionHandle}")
  def closeSession(@PathParam("sessionHandle") sessionHandleStr: String): Unit = {
    val resolved = resolveClient(sessionHandleStr)
    resolved.session match {
      case Some(session) => fe.be.closeSession(session.handle)
      case None => resolved.client.closeSession()
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = "text/event-stream")),
    description = "Send a message to the data agent and receive streaming SSE response")
  @POST
  @Path("{sessionHandle}/chat")
  def chat(
      @PathParam("sessionHandle") sessionHandleStr: String,
      request: ChatRequest,
      @Context response: HttpServletResponse): Unit = {
    if (request == null) {
      sendPreflightSseError(response, "request body is required")
      return
    }
    val text = request.getText
    if (text == null || text.trim.isEmpty) {
      sendPreflightSseError(response, "text is required")
      return
    }
    val model = Option(request.getModel).map(_.trim).filter(_.nonEmpty)
    if (model.exists(m => m.length > MAX_MODEL_LENGTH || !MODEL_PATTERN.matcher(m).matches())) {
      sendPreflightSseError(response, "invalid model name")
      return
    }
    val approvalMode = Option(request.getApprovalMode).map(_.trim.toUpperCase).filter(_.nonEmpty)
    if (approvalMode.exists(m => !VALID_APPROVAL_MODES.contains(m))) {
      sendPreflightSseError(
        response,
        s"invalid approvalMode, must be one of: ${VALID_APPROVAL_MODES.mkString(", ")}")
      return
    }
    var confOverlay = model
      .map(m => Map(KyuubiConf.ENGINE_DATA_AGENT_MODEL.key -> m))
      .getOrElse(Map.empty[String, String])
    approvalMode.foreach { mode =>
      confOverlay = confOverlay + (KyuubiConf.ENGINE_DATA_AGENT_APPROVAL_MODE.key -> mode)
    }

    // Validate and authorize before engine startup; launching can be expensive.
    val resolved = resolveClient(sessionHandleStr)
    val client = resolved.client
    val operationTimeoutMs = fe.getConf.get(KyuubiConf.FRONTEND_DATA_AGENT_OPERATION_TIMEOUT)

    val stream = new SseStream(response)
    val deadlineAt = System.currentTimeMillis() + STREAM_MAX_DURATION_MS
    stream.open()
    stream.keepalive()
    // Avoid logging raw user text; it may contain PII or control characters.
    info(s"Data Agent chat: session=$sessionHandleStr, textLen=${text.length}," +
      s" textHash=${Integer.toHexString(text.hashCode)}")

    // executeStatement may block before returning an op handle. Do not cancel the future on
    // timeout; late handles are closed by whenComplete so engine operations do not leak.
    val timedOut = new AtomicBoolean(false)
    val closed = new AtomicBoolean(false)
    val opSubmitFuture: CompletableFuture[TOperationHandle] =
      try {
        val f = CompletableFuture.supplyAsync(
          () => client.executeStatement(text, confOverlay, true, 0L),
          opSubmitter)
        f.whenComplete((handle, _) => {
          if (handle != null && timedOut.get() && closed.compareAndSet(false, true)) {
            info(s"Closing orphaned op for session $sessionHandleStr (servlet already timed out)")
            closeOperation(client, handle)
          }
        })
        f
      } catch {
        case _: RejectedExecutionException =>
          warn(s"Op-submit pool rejected chat for session $sessionHandleStr (queue full)")
          stream.event("error", buildJsonMessage("Server is busy, please retry"))
          stream.event("done", "{}")
          resolved.closeTransport()
          return
      }

    var opHandle: TOperationHandle = null
    try {
      try {
        opHandle = opSubmitFuture.get(operationTimeoutMs, TimeUnit.MILLISECONDS)
      } catch {
        case _: TimeoutException =>
          timedOut.set(true)
          // The worker may have completed just before timedOut was set.
          if (opSubmitFuture.isDone) {
            try {
              val orphan = opSubmitFuture.getNow(null)
              if (orphan != null && closed.compareAndSet(false, true)) {
                info(s"Closing orphaned op for session $sessionHandleStr (race recovery)")
                closeOperation(client, orphan)
              }
            } catch {
              case NonFatal(_) => // future completed exceptionally; nothing to close
            }
          }
          stream.event("error", buildJsonMessage("Operation submit timed out"))
          stream.event("done", "{}")
          return
        case e: ExecutionException =>
          val cause = Option(e.getCause).getOrElse(e)
          throw cause
      }
      streamResults(client, opHandle, stream, deadlineAt)
      stream.event("done", "{}")
    } catch {
      case _: IOException =>
        info(s"Client disconnected during SSE stream for session $sessionHandleStr")
        if (opHandle != null) cancelOperation(client, opHandle)
      case NonFatal(e) =>
        warn(s"Error during SSE streaming for session $sessionHandleStr", e)
        try {
          stream.event("error", buildJsonMessage(e.getMessage))
          stream.event("done", "{}")
        } catch {
          case _: IOException => // client already gone
        }
    } finally {
      if (opHandle != null) closeOperation(client, opHandle)
      resolved.closeTransport()
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description = "Approve or deny a pending tool call")
  @POST
  @Path("{sessionHandle}/approve")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def approve(
      @PathParam("sessionHandle") sessionHandleStr: String,
      request: ApprovalRequest): String = {
    if (request == null) {
      throw new WebApplicationException("request body is required", 400)
    }
    val requestId = request.getRequestId
    if (requestId == null || requestId.trim.isEmpty) {
      throw new WebApplicationException("requestId is required", 400)
    }
    // The id is embedded in `__approve:<id>` / `__deny:<id>`.
    if (requestId.length > MAX_REQUEST_ID_LENGTH ||
      !REQUEST_ID_PATTERN.matcher(requestId).matches()) {
      throw new WebApplicationException("invalid requestId", 400)
    }
    val resolved = resolveClient(sessionHandleStr)
    val client = resolved.client

    val statement = if (request.isApproved) {
      s"__approve:$requestId"
    } else {
      s"__deny:$requestId"
    }

    var opHandle: TOperationHandle = null
    try {
      opHandle = client.executeStatement(
        statement,
        Map.empty[String, String],
        false,
        60000L)
      val rowSet = client.fetchResults(opHandle, FetchOrientation.FETCH_NEXT, 1, false)
      val rows = extractStringRows(rowSet)
      rows.headOption.getOrElse {
        val node = JSON_MAPPER.createObjectNode()
        node.put("status", "error")
        node.put("requestId", requestId)
        node.put("message", "No result returned")
        JSON_MAPPER.writeValueAsString(node)
      }
    } finally {
      if (opHandle != null) closeOperation(client, opHandle)
      resolved.closeTransport()
    }
  }

  private def streamResults(
      client: KyuubiSyncThriftClient,
      opHandle: TOperationHandle,
      stream: SseStream,
      deadlineAt: Long): Unit = {
    waitForRunning(client, opHandle, stream, deadlineAt)

    var backoffMs = MIN_BACKOFF_MS
    var done = false
    while (!done) {
      if (System.currentTimeMillis() > deadlineAt) {
        cancelOperation(client, opHandle)
        stream.event("error", buildJsonMessage("stream exceeded maximum duration"))
        done = true
      } else {
        // Fetch can race terminal-state transitions; status decides retry vs. finish.
        val rows =
          try {
            fetchAndEmit(client, opHandle, stream)
          } catch {
            case _: KyuubiSQLException => 0
            case _: IllegalStateException => 0 // in-JVM engine path
          }
        if (rows > 0) {
          backoffMs = MIN_BACKOFF_MS
        } else {
          val status = client.getOperationStatus(opHandle)
          val opState = status.getOperationState
          if (isTerminalState(opState)) {
            if (opState == TOperationState.FINISHED_STATE) {
              // FINISHED may still have buffered rows; keep deadline as the cap.
              while (System.currentTimeMillis() < deadlineAt &&
                fetchAndEmit(client, opHandle, stream) > 0) {}
            }
            opState match {
              case TOperationState.FINISHED_STATE => // success -- no error frame
              case TOperationState.ERROR_STATE =>
                val errMsg = Option(status.getErrorMessage).getOrElse("Unknown error")
                stream.event("error", buildJsonMessage(errMsg))
              case TOperationState.CANCELED_STATE =>
                stream.event("error", buildJsonMessage("operation was canceled"))
              case TOperationState.TIMEDOUT_STATE =>
                stream.event("error", buildJsonMessage("operation timed out"))
              case TOperationState.CLOSED_STATE =>
                stream.event("error", buildJsonMessage("operation closed before completion"))
              case _ => // any other state isTerminalState() may grow to cover
            }
            done = true
          } else {
            if (stream.idleMs >= KEEPALIVE_INTERVAL_MS) stream.keepalive()
            Thread.sleep(backoffMs)
            backoffMs = math.min(backoffMs * 2, MAX_BACKOFF_MS)
          }
        }
      }
    }
  }

  private def waitForRunning(
      client: KyuubiSyncThriftClient,
      opHandle: TOperationHandle,
      stream: SseStream,
      deadlineAt: Long): Unit = {
    var sleepMs = 50L
    var ready = false
    while (!ready) {
      if (System.currentTimeMillis() > deadlineAt) {
        throw new IllegalStateException("Operation did not start within timeout")
      }
      val state = client.getOperationStatus(opHandle).getOperationState
      state match {
        case TOperationState.INITIALIZED_STATE | TOperationState.PENDING_STATE =>
          try {
            Thread.sleep(sleepMs)
          } catch {
            case _: InterruptedException =>
              Thread.currentThread().interrupt()
              throw new IllegalStateException("Interrupted while waiting for operation to start")
          }
          sleepMs = math.min(sleepMs * 2, 1000L)
          // Cold starts can be quiet longer than the browser watchdog.
          if (stream.idleMs >= KEEPALIVE_INTERVAL_MS) stream.keepalive()
        case _ =>
          ready = true
      }
    }
  }

  private def fetchAndEmit(
      client: KyuubiSyncThriftClient,
      opHandle: TOperationHandle,
      stream: SseStream): Int = {
    val rowSet = client.fetchResults(opHandle, FetchOrientation.FETCH_NEXT, 10, false)
    val rows = extractStringRows(rowSet)
    for (row <- rows) {
      stream.event(extractJsonType(row), row)
    }
    rows.size
  }

  private def extractJsonType(json: String): String = {
    try {
      val node = JSON_MAPPER.readTree(json)
      Option(node.get("type")).map(_.asText()).getOrElse("message")
    } catch {
      case NonFatal(_) => "message"
    }
  }

  private def extractStringRows(rowSet: TRowSet): Seq[String] = {
    if (rowSet == null) return Seq.empty
    // HS2 may encode the single string column as column-based or row-based data.
    val columns = rowSet.getColumns
    if (columns != null && !columns.isEmpty) {
      val stringCol = columns.get(0).getStringVal
      if (stringCol != null) {
        return stringCol.getValues.asScala.toSeq
      }
    }
    val rows = rowSet.getRows
    if (rows != null && !rows.isEmpty) {
      return rows.asScala.map { row =>
        val colVals = row.getColVals
        if (colVals != null && !colVals.isEmpty) {
          colVals.get(0).getStringVal.getValue
        } else ""
      }.toSeq
    }
    Seq.empty
  }

  private def isTerminalState(state: TOperationState): Boolean = {
    state == TOperationState.FINISHED_STATE ||
    state == TOperationState.CANCELED_STATE ||
    state == TOperationState.CLOSED_STATE ||
    state == TOperationState.ERROR_STATE ||
    state == TOperationState.TIMEDOUT_STATE
  }

  private def buildJsonMessage(message: String): String = {
    val node = JSON_MAPPER.createObjectNode()
    node.put("message", if (message == null) "" else message)
    JSON_MAPPER.writeValueAsString(node)
  }

  private def sendPreflightSseError(response: HttpServletResponse, message: String): Unit = {
    try {
      val s = new SseStream(response)
      s.open()
      s.event("error", buildJsonMessage(message))
      s.event("done", "{}")
    } catch {
      case _: IOException => // client already gone
    }
  }

  private def cancelOperation(
      client: KyuubiSyncThriftClient,
      opHandle: TOperationHandle): Unit = {
    try {
      client.cancelOperation(opHandle)
    } catch {
      case NonFatal(e) =>
        debug(s"Failed to cancel operation on client disconnect", e)
    }
  }

  private def closeOperation(
      client: KyuubiSyncThriftClient,
      opHandle: TOperationHandle): Unit = {
    try {
      client.closeOperation(opHandle)
    } catch {
      case NonFatal(e) =>
        debug(s"Failed to close operation", e)
    }
  }
}

private[server] object DataAgentResource {
  // Bounded pool for blocking executeStatement submissions; rebuildable after service restart.
  @volatile private var opSubmitExecutor: ExecutorService = newOpSubmitExecutor()

  private def newOpSubmitExecutor(): ExecutorService =
    ThreadUtils.newDaemonQueuedThreadPool(
      poolSize = 8,
      poolQueueSize = 64,
      keepAliveMs = 60000L,
      threadPoolName = "data-agent-op-submit")

  private def opSubmitter: ExecutorService = {
    val current = opSubmitExecutor
    if (current != null && !current.isShutdown) current
    else synchronized {
      if (opSubmitExecutor == null || opSubmitExecutor.isShutdown) {
        opSubmitExecutor = newOpSubmitExecutor()
      }
      opSubmitExecutor
    }
  }

  def shutdown(): Unit = synchronized {
    val current = opSubmitExecutor
    if (current != null) {
      ThreadUtils.shutdown(current)
      opSubmitExecutor = null
    }
  }

  private val JSON_MAPPER: ObjectMapper = new ObjectMapper()

  private val MAX_MODEL_LENGTH = 128
  private val MODEL_PATTERN: Pattern = Pattern.compile("^[a-zA-Z0-9._/:@-]+$")
  private val VALID_APPROVAL_MODES: Set[String] = Set("AUTO_APPROVE", "NORMAL", "STRICT")

  private val MAX_REQUEST_ID_LENGTH = 256
  private val REQUEST_ID_PATTERN: Pattern = Pattern.compile("^[A-Za-z0-9._:@-]+$")

  private val MIN_BACKOFF_MS = 50L
  private val MAX_BACKOFF_MS = 500L
  private val KEEPALIVE_INTERVAL_MS = 15000L
  private val STREAM_MAX_DURATION_MS = 10L * 60L * 1000L

  final private[v1] class SseStream(response: HttpServletResponse) {
    private val out = response.getOutputStream
    private val writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)
    private var lastWriteMs = System.currentTimeMillis()

    def open(): Unit = {
      response.setBufferSize(0) // disable Jetty output buffering for streaming
      response.setStatus(HttpServletResponse.SC_OK)
      response.setContentType("text/event-stream")
      response.setCharacterEncoding("UTF-8")
      response.setHeader("Cache-Control", "no-cache")
      response.setHeader("Connection", "keep-alive")
      response.setHeader("X-Accel-Buffering", "no")
      response.flushBuffer()
    }

    def event(name: String, data: String): Unit = {
      writer.write(s"event: $name\n")
      // SSE requires one data field per physical payload line.
      for (line <- data.split("\n", -1)) {
        writer.write(s"data: $line\n")
      }
      writer.write("\n")
      writer.flush()
      lastWriteMs = System.currentTimeMillis()
    }

    def keepalive(): Unit = {
      // Named events reach fetch-event-source; comment frames do not.
      writer.write("event: ping\ndata: {}\n\n")
      writer.flush()
      lastWriteMs = System.currentTimeMillis()
    }

    def idleMs: Long = System.currentTimeMillis() - lastWriteMs
  }
}
