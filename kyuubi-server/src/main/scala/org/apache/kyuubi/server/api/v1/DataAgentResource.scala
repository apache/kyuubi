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
import org.apache.kyuubi.operation.FetchOrientation
import org.apache.kyuubi.server.api.ApiRequestContext
import org.apache.kyuubi.session.{KyuubiSessionImpl, SessionHandle}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.util.ThreadUtils

@Tag(name = "DataAgent")
@Consumes(Array(MediaType.APPLICATION_JSON))
private[v1] class DataAgentResource extends ApiRequestContext with Logging {

  import DataAgentResource._

  private def verifySessionOwnership(session: KyuubiSessionImpl): Unit = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    if (!fe.isAdministrator(userName) && session.user != userName) {
      throw new ForbiddenException(
        s"$userName is not allowed to access session ${session.handle}")
    }
  }

  /**
   * Resolve a session handle, verify ownership, and return the session. Failures throw
   * WebApplicationException with the appropriate HTTP status so Jersey can return a proper
   * 4xx instead of a 200 SSE stream -- important for clients fishing for foreign session IDs
   * (they should see 403/404, not engine error messages echoed back through SSE).
   */
  private def resolveAndAuthorize(sessionHandleStr: String): KyuubiSessionImpl = {
    val sessionHandle =
      try {
        SessionHandle.fromUUID(sessionHandleStr)
      } catch {
        case _: IllegalArgumentException =>
          throw new WebApplicationException("invalid sessionHandle", 400)
      }
    val session =
      try {
        fe.be.sessionManager.getSession(sessionHandle).asInstanceOf[KyuubiSessionImpl]
      } catch {
        case _: KyuubiSQLException =>
          throw new WebApplicationException("session not found", 404)
      }
    verifySessionOwnership(session)
    session
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
    // Phase 1: validate request body BEFORE any session lookup or engine launch. Bad input
    // shouldn't trigger expensive engine startup.
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

    // Phase 2: resolve session and wait for the engine client. Auth/lookup failures throw
    // WebApplicationException with proper 4xx status BEFORE any SSE bytes are sent, so a
    // caller probing foreign session IDs sees 403/404 rather than an engine error inside a
    // 200 SSE frame. Engine launch / readiness failures are expected and surface as SSE errors
    // (the Web UI already parses them).
    val session = resolveAndAuthorize(sessionHandleStr)
    val operationTimeoutMs = fe.getConf.get(KyuubiConf.FRONTEND_DATA_AGENT_OPERATION_TIMEOUT)
    val client: KyuubiSyncThriftClient =
      try {
        val launchOp = session.launchEngineOp
        try {
          launchOp.getBackgroundHandle.get(operationTimeoutMs, TimeUnit.MILLISECONDS)
        } catch {
          case _: TimeoutException =>
            sendPreflightSseError(response, "Engine did not start within timeout")
            return
          case e: ExecutionException =>
            val errMsg = Option(e.getCause).map(_.getMessage).getOrElse("Engine launch failed")
            sendPreflightSseError(response, errMsg)
            return
        }
        val c = session.client
        if (c == null) {
          sendPreflightSseError(response, "Engine session is not ready after waiting")
          return
        }
        c
      } catch {
        case NonFatal(e) =>
          error(s"Error processing chat for session $sessionHandleStr", e)
          if (!response.isCommitted) sendPreflightSseError(response, e.getMessage)
          return
      }

    // Phase 3: open SSE stream and submit operation with bounded wait. The stream is opened
    // BEFORE executeStatement so any failure surfaces as an SSE error event rather than
    // leaving the client hung on an already-committed 200 OK.
    val stream = new SseStream(response)
    val deadlineAt = System.currentTimeMillis() + STREAM_MAX_DURATION_MS
    stream.open()
    // Emit an immediate ping so the client's watchdog has a known "stream established"
    // anchor while we wait for executeStatement to return an operation handle.
    stream.keepalive()
    // Don't log raw user text -- it's user-supplied and may contain PII or newlines (log
    // injection). Length + hash is enough to correlate a specific request across log lines.
    info(s"Data Agent chat: session=$sessionHandleStr, textLen=${text.length}," +
      s" textHash=${Integer.toHexString(text.hashCode)}")

    // executeStatement is synchronous and has no built-in RPC timeout. If the engine hangs
    // before returning an opHandle the servlet thread would block indefinitely, so we run it
    // on a dedicated cached pool (not the JDK common ForkJoinPool, which is sized for CPU
    // work and would starve other parallel-stream callers).
    //
    // CompletableFuture.cancel cannot interrupt an already-running supplyAsync task. So if
    // our timeout fires but the worker eventually returns an opHandle, that handle is
    // orphaned -- nobody on the servlet path is listening anymore.
    //
    // Two parties may want to close the orphan: the whenComplete callback (worker thread)
    // and the timeout-recovery branch below (servlet thread). They race on `closed` so
    // exactly one wins. `timedOut` is set BEFORE either party reads it: if whenComplete
    // fires before the servlet sees TimeoutException, it sees timedOut=false and does
    // nothing -- the recovery branch will then notice the future already completed and
    // close the handle itself.
    val timedOut = new AtomicBoolean(false)
    val closed = new AtomicBoolean(false)
    val opSubmitFuture: CompletableFuture[TOperationHandle] =
      try {
        val f = CompletableFuture.supplyAsync(
          () => client.executeStatement(text, confOverlay, true, 0L),
          OP_SUBMIT_EXECUTOR)
        f.whenComplete((handle, _) => {
          if (handle != null && timedOut.get() && closed.compareAndSet(false, true)) {
            info(s"Closing orphaned op for session $sessionHandleStr (servlet already timed out)")
            closeOperation(client, handle)
          }
        })
        f
      } catch {
        case _: RejectedExecutionException =>
          // Bounded submit pool is full -- shed load with a clean SSE error rather than
          // leaving the client on a half-open stream.
          warn(s"Op-submit pool rejected chat for session $sessionHandleStr (queue full)")
          stream.event("error", buildJsonMessage("Server is busy, please retry"))
          stream.event("done", "{}")
          return
      }

    var opHandle: TOperationHandle = null
    try {
      try {
        opHandle = opSubmitFuture.get(operationTimeoutMs, TimeUnit.MILLISECONDS)
      } catch {
        case _: TimeoutException =>
          timedOut.set(true)
          opSubmitFuture.cancel(true)
          // Recover from the race where the worker completed between get() throwing and
          // timedOut being set: whenComplete already ran with timedOut=false and skipped
          // cleanup, so we close the orphan here.
          if (opSubmitFuture.isDone && !opSubmitFuture.isCancelled) {
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
    val session = resolveAndAuthorize(sessionHandleStr)
    val client = session.client
    if (client == null) {
      throw new WebApplicationException("Engine session is not ready", 503)
    }

    val requestId = request.getRequestId
    if (requestId == null || requestId.trim.isEmpty) {
      throw new WebApplicationException("requestId is required", 400)
    }
    // Engine-generated request IDs are short alphanumeric tokens. The string is concatenated
    // into a colon-delimited engine command (`__approve:<id>`), so reject anything that could
    // break that protocol or arrive with newlines / unbounded length.
    if (requestId.length > MAX_REQUEST_ID_LENGTH ||
      !REQUEST_ID_PATTERN.matcher(requestId).matches()) {
      throw new WebApplicationException("invalid requestId", 400)
    }

    val statement = if (request.isApproved) {
      s"__approve:$requestId"
    } else {
      s"__deny:$requestId"
    }

    val opHandle = client.executeStatement(
      statement,
      Map.empty[String, String],
      false,
      60000L)
    try {
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
      closeOperation(client, opHandle)
    }
  }

  /**
   * Single-loop polling driver. The engine's fetch iterator can't distinguish "drained but
   * still running" from "drained and terminal", so we have to cross-check operation status
   * when a fetch returns empty -- that's why this is a poll loop rather than a pipe.
   *
   * Invariants:
   *   - `deadlineAt` bounds the whole stream lifecycle (including waitForRunning).
   *   - On terminal state we always drain remaining rows BEFORE emitting a terminal error,
   *     so clients see data in logical order.
   *   - Keepalive comments (`: ka`) only fire in the idle branch and reset the idle clock,
   *     so an active stream never sees them.
   */
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
        // The data agent engine permits fetch only in RUNNING and FINISHED. If the op
        // transitions to a failure terminal between two polls, the engine raises
        // IllegalStateException -- but the Thrift layer wraps it into a TStatus error,
        // and KyuubiSyncThriftClient.fetchResults rethrows it as KyuubiSQLException.
        // Treat any thrift-side fetch failure as "go check status" so the engine's real
        // error reaches the client; the status branch decides whether it's a true
        // terminal or a transient race that warrants retry.
        val (rows, fetchFailed) =
          try {
            (fetchAndEmit(client, opHandle, stream), false)
          } catch {
            case _: KyuubiSQLException => (0, true)
            case _: IllegalStateException => (0, true) // in-JVM engine path
          }
        if (rows > 0) {
          backoffMs = MIN_BACKOFF_MS
        } else {
          val status = client.getOperationStatus(opHandle)
          val opState = status.getOperationState
          if (isTerminalState(opState)) {
            // Only FINISHED still allows fetch; in all other terminals the engine rejects
            // fetch, so any rows produced after the previous poll are unrecoverable. We
            // surface the engine-reported error message instead of the raw fetch exception.
            if (opState == TOperationState.FINISHED_STATE) {
              // Bound the drain by deadline too -- a pathological engine that keeps emitting
              // rows in FINISHED state must not let us run past STREAM_MAX_DURATION_MS.
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
          } else if (fetchFailed) {
            // Non-terminal but fetch refused -- race window between status polls; back off.
            if (stream.idleMs >= KEEPALIVE_INTERVAL_MS) stream.keepalive()
            Thread.sleep(backoffMs)
            backoffMs = math.min(backoffMs * 2, MAX_BACKOFF_MS)
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
          // Cold-start can sit in PENDING longer than the client-side watchdog (30s in the
          // Web UI). Emit a ping when we cross the keepalive interval so the watchdog stays
          // armed; otherwise the client tears down the stream right as the engine becomes
          // ready.
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

  /** Extract the "type" field from a JSON string for use as SSE event name. */
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
    // Engine emits a single string column. HS2 protocol >= V6 uses column-based TRowSet,
    // earlier versions use row-based; sessions opened via the REST default to V1 today.
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

  /** Build a JSON object with a single "message" field using Jackson to guarantee valid JSON. */
  private def buildJsonMessage(message: String): String = {
    val node = JSON_MAPPER.createObjectNode()
    node.put("message", if (message == null) "" else message)
    JSON_MAPPER.writeValueAsString(node)
  }

  /** Send an error event for preflight failures (before the stream has started emitting data). */
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

private[v1] object DataAgentResource {
  // Dedicated bounded pool for blocking executeStatement submissions. Avoids the JDK common
  // pool (sized for CPU work, would starve parallel-stream callers if we parked Thrift RPCs
  // on it) and avoids an unbounded cached pool (a stuck engine could otherwise grow daemon
  // threads without limit). Pool/queue sizes are deliberate: hot path returns in milliseconds,
  // so 8 workers + 64 queued submits is plenty for normal load and rejects fast under abuse.
  private val OP_SUBMIT_EXECUTOR: ExecutorService = ThreadUtils.newDaemonQueuedThreadPool(
    poolSize = 8,
    poolQueueSize = 64,
    keepAliveMs = 60000L,
    threadPoolName = "data-agent-op-submit")

  // Jersey instantiates resources per-request by default; ObjectMapper is heavyweight and
  // thread-safe, so share one across all DataAgentResource instances.
  private val JSON_MAPPER: ObjectMapper = new ObjectMapper()

  private val MAX_MODEL_LENGTH = 128
  // Alphanumeric, hyphens, underscores, dots, slashes, and colons (covers e.g. "gpt-4o",
  // "deepseek-chat", "accounts/fireworks/models/llama-v3-70b")
  private val MODEL_PATTERN: Pattern = Pattern.compile("^[a-zA-Z0-9._/:@-]+$")
  private val VALID_APPROVAL_MODES: Set[String] = Set("AUTO_APPROVE", "NORMAL", "STRICT")

  // Approval requestIds are concatenated into the engine command `__approve:<id>`. Constrain
  // length and character set so external callers can't break command parsing.
  private val MAX_REQUEST_ID_LENGTH = 256
  private val REQUEST_ID_PATTERN: Pattern = Pattern.compile("^[A-Za-z0-9._:@-]+$")

  private val MIN_BACKOFF_MS = 50L
  private val MAX_BACKOFF_MS = 500L
  // 15s keepalive stays well under the common nginx/ALB 60s idle timeout.
  private val KEEPALIVE_INTERVAL_MS = 15000L
  // Hard ceiling on a single chat stream. Beyond this we cancel the op and surface an error.
  private val STREAM_MAX_DURATION_MS = 10L * 60L * 1000L

  /**
   * Scoped wrapper around the servlet response that owns all SSE byte-level concerns:
   * headers, event framing, and keepalive comments. One instance per request; not thread-safe.
   */
  final private class SseStream(response: HttpServletResponse) {
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
      // Per SSE spec, each physical newline in the payload becomes its own `data:` field.
      for (line <- data.split("\n", -1)) {
        writer.write(s"data: $line\n")
      }
      writer.write("\n")
      writer.flush()
      lastWriteMs = System.currentTimeMillis()
    }

    /**
     * Heartbeat as a named `ping` event. Unlike SSE comment frames (`: ka`), named events
     * reach the browser's EventSource dispatcher, so clients can use them to distinguish
     * "backend alive but quiet" from "backend gone" and drive their own watchdog timer.
     */
    def keepalive(): Unit = {
      writer.write("event: ping\ndata: {}\n\n")
      writer.flush()
      lastWriteMs = System.currentTimeMillis()
    }

    def idleMs: Long = System.currentTimeMillis() - lastWriteMs
  }
}
