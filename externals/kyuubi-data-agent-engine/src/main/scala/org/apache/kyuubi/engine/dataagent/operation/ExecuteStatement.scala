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
package org.apache.kyuubi.engine.dataagent.operation

import java.util.concurrent.RejectedExecutionException

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.slf4j.MDC

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.dataagent.provider.{DataAgentProvider, ProviderRunRequest}
import org.apache.kyuubi.engine.dataagent.runtime.event.{AgentError, AgentEvent, AgentFinish, ApprovalRequest, ContentDelta, EventType, StepEnd, StepStart, ToolCall, ToolResult}
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

class ExecuteStatement(
    session: Session,
    override val statement: String,
    confOverlay: Map[String, String],
    override val shouldRunAsync: Boolean,
    queryTimeout: Long,
    dataAgentProvider: DataAgentProvider)
  extends DataAgentOperation(session) with Logging {

  import ExecuteStatement.JSON

  private val operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)
  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  private val incrementalIter = new IncrementalFetchIterator[Array[String]]()

  override protected def runInternal(): Unit = {
    addTimeoutMonitor(queryTimeout)
    iter = incrementalIter

    val asyncOperation = new Runnable {
      override def run(): Unit = {
        executeStatement()
      }
    }

    try {
      val sessionManager = session.sessionManager
      val backgroundHandle = sessionManager.submitBackgroundOperation(asyncOperation)
      setBackgroundHandle(backgroundHandle)
    } catch {
      case rejected: RejectedExecutionException =>
        setState(OperationState.ERROR)
        val ke =
          KyuubiSQLException("Error submitting query in background, query rejected", rejected)
        setOperationException(ke)
        shutdownTimeoutMonitor()
        throw ke
    }
  }

  private def toJson(build: ObjectNode => Unit): String = {
    val node = JSON.createObjectNode()
    build(node)
    JSON.writeValueAsString(node)
  }

  private def executeStatement(): Unit = {
    setState(OperationState.RUNNING)

    try {
      val sessionId = session.handle.identifier.toString
      val operationId = getHandle.identifier.toString
      MDC.put("operationId", operationId)
      MDC.put("sessionId", sessionId)
      val request = new ProviderRunRequest(statement)
      // Merge session-level conf with per-statement confOverlay (overlay takes precedence)
      val mergedConf = session.conf ++ confOverlay
      mergedConf.get(KyuubiConf.ENGINE_DATA_AGENT_LLM_MODEL.key).foreach(request.modelName)
      val approvalMode = mergedConf.getOrElse(
        KyuubiConf.ENGINE_DATA_AGENT_APPROVAL_MODE.key,
        session.sessionManager.getConf.get(KyuubiConf.ENGINE_DATA_AGENT_APPROVAL_MODE))
      request.approvalMode(approvalMode)

      val eventConsumer: AgentEvent => Unit = { (event: AgentEvent) =>
        val sseType = event.eventType().sseEventName()
        event.eventType() match {
          case EventType.AGENT_START =>
            incrementalIter.append(Array(toJson { n =>
              n.put("type", sseType)
            }))
          case EventType.STEP_START =>
            val stepStart = event.asInstanceOf[StepStart]
            incrementalIter.append(Array(toJson { n =>
              n.put("type", sseType); n.put("step", stepStart.stepNumber())
            }))
          case EventType.CONTENT_DELTA =>
            val delta = event.asInstanceOf[ContentDelta]
            incrementalIter.append(Array(toJson { n =>
              n.put("type", sseType); n.put("text", delta.text())
            }))
          case EventType.TOOL_CALL =>
            val toolCall = event.asInstanceOf[ToolCall]
            incrementalIter.append(Array(toJson { n =>
              n.put("type", sseType)
              n.put("id", toolCall.toolCallId())
              n.put("name", toolCall.toolName())
              n.set("args", JSON.valueToTree(toolCall.toolArgs()))
            }))
          case EventType.TOOL_RESULT =>
            val toolResult = event.asInstanceOf[ToolResult]
            incrementalIter.append(Array(toJson { n =>
              n.put("type", sseType)
              n.put("id", toolResult.toolCallId())
              n.put("name", toolResult.toolName())
              n.put("output", toolResult.output())
              n.put("isError", toolResult.isError())
            }))
          case EventType.STEP_END =>
            val stepEnd = event.asInstanceOf[StepEnd]
            incrementalIter.append(Array(toJson { n =>
              n.put("type", sseType); n.put("step", stepEnd.stepNumber())
            }))
          case EventType.ERROR =>
            val err = event.asInstanceOf[AgentError]
            incrementalIter.append(Array(toJson { n =>
              n.put("type", sseType); n.put("message", err.message())
            }))
          case EventType.APPROVAL_REQUEST =>
            val req = event.asInstanceOf[ApprovalRequest]
            incrementalIter.append(Array(toJson { n =>
              n.put("type", sseType)
              n.put("requestId", req.requestId())
              n.put("id", req.toolCallId())
              n.put("name", req.toolName())
              n.set("args", JSON.valueToTree(req.toolArgs()))
              n.put("riskLevel", req.riskLevel().name())
            }))
          case EventType.AGENT_FINISH =>
            val finish = event.asInstanceOf[AgentFinish]
            incrementalIter.append(Array(toJson { n =>
              n.put("type", sseType)
              n.put("steps", finish.totalSteps())
            }))
          case _ => // CONTENT_COMPLETE — internal to middleware pipeline
        }
      }
      dataAgentProvider.run(sessionId, request, e => eventConsumer(e))

      setState(OperationState.FINISHED)
    } catch {
      onError()
    } finally {
      MDC.remove("operationId")
      MDC.remove("sessionId")
      shutdownTimeoutMonitor()
    }
  }
}

object ExecuteStatement {
  private val JSON = new ObjectMapper()
}
