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
import java.util.concurrent.{ExecutionException, TimeoutException, TimeUnit}
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

import org.apache.kyuubi.Logging
import org.apache.kyuubi.client.api.v1.dto.{ApprovalRequest, ChatRequest}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.FetchOrientation
import org.apache.kyuubi.server.api.ApiRequestContext
import org.apache.kyuubi.session.{KyuubiSessionImpl, SessionHandle}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

@Tag(name = "DataAgent")
@Consumes(Array(MediaType.APPLICATION_JSON))
private[v1] class DataAgentResource extends ApiRequestContext with Logging {

  import DataAgentResource._

  private val jsonMapper = new ObjectMapper()

  private def verifySessionOwnership(session: KyuubiSessionImpl): Unit = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    if (!fe.isAdministrator(userName) && session.user != userName) {
      throw new ForbiddenException(
        s"$userName is not allowed to access session ${session.handle}")
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
    try {
      val sessionHandle = SessionHandle.fromUUID(sessionHandleStr)
      val session = fe.be.sessionManager.getSession(sessionHandle)
        .asInstanceOf[KyuubiSessionImpl]
      verifySessionOwnership(session)

      // Wait for the engine client to become ready using the launch operation's Future,
      // consistent with how KyuubiSessionImpl.waitForEngineLaunched() works.
      val operationTimeoutMs = fe.getConf.get(KyuubiConf.FRONTEND_DATA_AGENT_OPERATION_TIMEOUT)
      val launchOp = session.launchEngineOp
      try {
        launchOp.getBackgroundHandle.get(operationTimeoutMs, TimeUnit.MILLISECONDS)
      } catch {
        case _: TimeoutException =>
          sendSseError(response, "Engine did not start within timeout")
          return
        case e: ExecutionException =>
          val errMsg = Option(e.getCause).map(_.getMessage).getOrElse("Engine launch failed")
          sendSseError(response, errMsg)
          return
      }
      val client = session.client

      if (client == null) {
        sendSseError(response, "Engine session is not ready after waiting")
        return
      }

      val text = request.getText
      if (text == null || text.trim.isEmpty) {
        sendSseError(response, "text is required")
        return
      }
      if (text.length > MAX_TEXT_LENGTH) {
        sendSseError(
          response,
          s"text exceeds maximum length of $MAX_TEXT_LENGTH characters")
        return
      }

      // Validate optional model name - reject obviously invalid values
      val model = Option(request.getModel).map(_.trim).filter(_.nonEmpty)
      if (model.exists(m => m.length > MAX_MODEL_LENGTH || !MODEL_PATTERN.matcher(m).matches())) {
        sendSseError(response, "invalid model name")
        return
      }

      // Validate optional approval mode
      val approvalMode = Option(request.getApprovalMode).map(_.trim).filter(_.nonEmpty)
      if (approvalMode.exists(m => !VALID_APPROVAL_MODES.contains(m.toUpperCase))) {
        sendSseError(
          response,
          s"invalid approvalMode, must be one of: ${VALID_APPROVAL_MODES.mkString(", ")}")
        return
      }

      // Set SSE response headers before writing any data
      response.setBufferSize(0) // disable Jetty output buffering for streaming
      response.setStatus(HttpServletResponse.SC_OK)
      response.setContentType("text/event-stream")
      response.setCharacterEncoding("UTF-8")
      response.setHeader("Cache-Control", "no-cache")
      response.setHeader("Connection", "keep-alive")
      response.setHeader("X-Accel-Buffering", "no")
      response.flushBuffer()

      val outputStream = response.getOutputStream
      val writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)

      // Execute statement asynchronously on the engine
      info(s"Data Agent chat: session=$sessionHandleStr, text=${text.take(100)}")
      var confOverlay = model
        .map(m => Map("kyuubi.engine.data.agent.llm.model" -> m))
        .getOrElse(Map.empty[String, String])
      approvalMode.foreach { mode =>
        confOverlay = confOverlay + ("kyuubi.engine.data.agent.approval.mode" -> mode)
      }
      val opHandle = client.executeStatement(text, confOverlay, true, 0L)

      try {
        streamResults(client, opHandle, writer, outputStream)
      } catch {
        case _: IOException =>
          info(s"Client disconnected during SSE stream for session $sessionHandleStr")
          cancelOperation(client, opHandle)
        case NonFatal(e) =>
          warn(s"Error during SSE streaming for session $sessionHandleStr", e)
          try {
            writeSseEvent(
              writer,
              outputStream,
              "error",
              buildJsonMessage(e.getMessage))
          } catch {
            case _: IOException => // client already gone
          }
      } finally {
        closeOperation(client, opHandle)
      }
    } catch {
      case NonFatal(e) =>
        error(s"Error processing chat for session $sessionHandleStr", e)
        if (!response.isCommitted) {
          sendSseError(response, e.getMessage)
        }
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
    val sessionHandle = SessionHandle.fromUUID(sessionHandleStr)
    val session = fe.be.sessionManager.getSession(sessionHandle)
      .asInstanceOf[KyuubiSessionImpl]
    verifySessionOwnership(session)
    val client = session.client
    if (client == null) {
      throw new WebApplicationException("Engine session is not ready", 503)
    }

    val requestId = request.getRequestId
    if (requestId == null || requestId.trim.isEmpty) {
      throw new WebApplicationException("requestId is required", 400)
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
        val node = jsonMapper.createObjectNode()
        node.put("status", "error")
        node.put("requestId", requestId)
        node.put("message", "No result returned")
        jsonMapper.writeValueAsString(node)
      }
    } finally {
      closeOperation(client, opHandle)
    }
  }

  private def streamResults(
      client: org.apache.kyuubi.client.KyuubiSyncThriftClient,
      opHandle: TOperationHandle,
      writer: OutputStreamWriter,
      outputStream: javax.servlet.ServletOutputStream): Unit = {
    // Wait for operation to leave PENDING state before fetching
    waitForRunning(client, opHandle)

    // Phase 1: Poll while operation is running with adaptive backoff
    var operationDone = false
    var sleepMs = 50L
    while (!operationDone) {
      val rows = fetchAndEmit(client, opHandle, writer, outputStream)

      if (rows > 0) {
        sleepMs = 50L // reset to fast polling when messages are flowing
      } else {
        val status = client.getOperationStatus(opHandle)
        val opState = status.getOperationState
        if (isTerminalState(opState)) {
          if (opState == TOperationState.ERROR_STATE) {
            val errMsg = Option(status.getErrorMessage).getOrElse("Unknown error")
            writeSseEvent(writer, outputStream, "error", buildJsonMessage(errMsg))
          }
          operationDone = true
        } else {
          Thread.sleep(sleepMs)
          sleepMs = Math.min(sleepMs * 2, 500L) // backoff when idle
        }
      }
    }

    // Phase 2: Drain remaining data after operation finishes
    var hasMore = true
    while (hasMore) {
      hasMore = fetchAndEmit(client, opHandle, writer, outputStream) > 0
    }

    writeSseEvent(writer, outputStream, "done", "{}")
  }

  private def waitForRunning(
      client: org.apache.kyuubi.client.KyuubiSyncThriftClient,
      opHandle: TOperationHandle): Unit = {
    val operationTimeoutMs = fe.getConf.get(KyuubiConf.FRONTEND_DATA_AGENT_OPERATION_TIMEOUT)
    val deadline = System.currentTimeMillis() + operationTimeoutMs
    var sleepMs = 50L
    var ready = false
    while (!ready) {
      if (System.currentTimeMillis() > deadline) {
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
          sleepMs = Math.min(sleepMs * 2, 1000)
        case _ =>
          ready = true
      }
    }
  }

  private def fetchAndEmit(
      client: org.apache.kyuubi.client.KyuubiSyncThriftClient,
      opHandle: TOperationHandle,
      writer: OutputStreamWriter,
      outputStream: javax.servlet.ServletOutputStream): Int = {
    val rowSet = client.fetchResults(opHandle, FetchOrientation.FETCH_NEXT, 10, false)
    val rows = extractStringRows(rowSet)
    for (row <- rows) {
      val eventType = extractJsonType(row)
      writeSseEvent(writer, outputStream, eventType, row)
    }
    rows.size
  }

  /** Extract the "type" field from a JSON string for use as SSE event name. */
  private def extractJsonType(json: String): String = {
    try {
      val node = jsonMapper.readTree(json)
      Option(node.get("type")).map(_.asText()).getOrElse("message")
    } catch {
      case NonFatal(_) => "message"
    }
  }

  private def extractStringRows(rowSet: TRowSet): Seq[String] = {
    if (rowSet == null) return Seq.empty
    // Engine returns column-based format with a single string column
    val columns = rowSet.getColumns
    if (columns != null && !columns.isEmpty) {
      val stringCol = columns.get(0).getStringVal
      if (stringCol != null) {
        return stringCol.getValues.asScala.toSeq
      }
    }
    // Fallback: row-based format
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

  /**
   * Write a single SSE frame. If `data` contains newlines, each line
   * is written as a separate `data:` field per the SSE specification.
   */
  private def writeSseEvent(
      writer: OutputStreamWriter,
      outputStream: javax.servlet.ServletOutputStream,
      event: String,
      data: String): Unit = {
    writer.write(s"event: $event\n")
    for (line <- data.split("\n", -1)) {
      writer.write(s"data: $line\n")
    }
    writer.write("\n")
    writer.flush()
    outputStream.flush() // force Jetty to send the chunk to the network
  }

  /** Build a JSON object with a single "message" field using Jackson to guarantee valid JSON. */
  private def buildJsonMessage(message: String): String = {
    val node = jsonMapper.createObjectNode()
    node.put("message", if (message == null) "" else message)
    jsonMapper.writeValueAsString(node)
  }

  /**
   * Send an error as an SSE event so that fetch-event-source clients can parse it.
   * This avoids the content-type mismatch that occurs when returning JSON from an SSE endpoint.
   */
  private def sendSseError(
      response: HttpServletResponse,
      message: String): Unit = {
    response.setStatus(HttpServletResponse.SC_OK)
    response.setContentType("text/event-stream")
    response.setCharacterEncoding("UTF-8")
    response.setHeader("Cache-Control", "no-cache")
    val outputStream = response.getOutputStream
    val writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)
    writeSseEvent(writer, outputStream, "error", buildJsonMessage(message))
    writeSseEvent(writer, outputStream, "done", "{}")
  }

  private def sendJsonError(
      response: HttpServletResponse,
      status: Int,
      message: String): Unit = {
    response.setStatus(status)
    response.setContentType("application/json")
    response.setCharacterEncoding("UTF-8")
    response.getWriter.write(buildJsonMessage(message))
    response.getWriter.flush()
  }

  private def cancelOperation(
      client: org.apache.kyuubi.client.KyuubiSyncThriftClient,
      opHandle: TOperationHandle): Unit = {
    try {
      client.cancelOperation(opHandle)
    } catch {
      case NonFatal(e) =>
        debug(s"Failed to cancel operation on client disconnect", e)
    }
  }

  private def closeOperation(
      client: org.apache.kyuubi.client.KyuubiSyncThriftClient,
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
  private val MAX_TEXT_LENGTH = 32768
  private val MAX_MODEL_LENGTH = 128
  // Alphanumeric, hyphens, underscores, dots, slashes, and colons (covers e.g. "gpt-4o",
  // "deepseek-chat", "accounts/fireworks/models/llama-v3-70b")
  private val MODEL_PATTERN: Pattern = Pattern.compile("^[a-zA-Z0-9._/:@-]+$")
  private val VALID_APPROVAL_MODES: Set[String] = Set("AUTO_APPROVE", "NORMAL", "STRICT")
}
