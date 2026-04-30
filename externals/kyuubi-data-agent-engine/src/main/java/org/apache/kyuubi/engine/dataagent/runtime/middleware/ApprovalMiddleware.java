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

package org.apache.kyuubi.engine.dataagent.runtime.middleware;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kyuubi.engine.dataagent.runtime.AgentRunContext;
import org.apache.kyuubi.engine.dataagent.runtime.ApprovalMode;
import org.apache.kyuubi.engine.dataagent.runtime.event.ApprovalRequest;
import org.apache.kyuubi.engine.dataagent.tool.ToolRegistry;
import org.apache.kyuubi.engine.dataagent.tool.ToolRiskLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Middleware that enforces human-in-the-loop approval for tool calls based on the {@link
 * ApprovalMode} and the tool's {@link ToolRiskLevel}.
 *
 * <p>When approval is required, an {@link ApprovalRequest} event is emitted to the client via
 * {@link AgentRunContext#emit}, and the agent thread blocks until the client responds via {@link
 * #resolve} or the timeout expires.
 */
public class ApprovalMiddleware implements AgentMiddleware {

  private static final Logger LOG = LoggerFactory.getLogger(ApprovalMiddleware.class);

  private static final long DEFAULT_TIMEOUT_SECONDS = 300; // 5 minutes

  private final long timeoutSeconds;
  private final ConcurrentHashMap<String, CompletableFuture<Boolean>> pending =
      new ConcurrentHashMap<>();
  private ToolRegistry toolRegistry;

  public ApprovalMiddleware() {
    this(DEFAULT_TIMEOUT_SECONDS);
  }

  public ApprovalMiddleware(long timeoutSeconds) {
    this.timeoutSeconds = timeoutSeconds;
  }

  @Override
  public void onRegister(ToolRegistry registry) {
    this.toolRegistry = registry;
  }

  @Override
  public ToolCallAction beforeToolCall(
      AgentRunContext ctx, String toolCallId, String toolName, Map<String, Object> toolArgs) {
    ToolRiskLevel riskLevel = toolRegistry.getRiskLevel(toolName);

    if (shouldAutoApprove(ctx.getApprovalMode(), riskLevel)) {
      return ToolCallApproval.INSTANCE;
    }

    String requestId = UUID.randomUUID().toString();
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    pending.put(requestId, future);

    ctx.emit(new ApprovalRequest(requestId, toolCallId, toolName, toolArgs, riskLevel));
    LOG.info("Approval requested for tool '{}' (requestId={})", toolName, requestId);

    try {
      boolean approved = future.get(timeoutSeconds, TimeUnit.SECONDS);
      if (!approved) {
        LOG.info("Tool '{}' denied by user (requestId={})", toolName, requestId);
        return new ToolCallDenial("User denied execution of " + toolName);
      }
      LOG.info("Tool '{}' approved by user (requestId={})", toolName, requestId);
      return ToolCallApproval.INSTANCE;
    } catch (TimeoutException e) {
      // Complete the future so that a late resolve() call is a harmless no-op
      // instead of completing a dangling future.
      future.completeExceptionally(e);
      LOG.warn("Approval timed out for tool '{}' (requestId={})", toolName, requestId);
      return new ToolCallDenial("Approval timed out after " + timeoutSeconds + "s for " + toolName);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return new ToolCallDenial("Approval interrupted for " + toolName);
    } catch (Exception e) {
      LOG.error("Unexpected error waiting for approval", e);
      return new ToolCallDenial("Approval error: " + e.getMessage());
    } finally {
      pending.remove(requestId);
    }
  }

  /**
   * Resolve a pending approval request. Called by the external approval channel (e.g. a Kyuubi
   * operation or REST endpoint).
   *
   * @param requestId the request ID from the {@link ApprovalRequest} event
   * @param approved true to approve, false to deny
   * @return true if the request was found and resolved, false if not found (already timed out or
   *     invalid ID)
   */
  public boolean resolve(String requestId, boolean approved) {
    CompletableFuture<Boolean> future = pending.get(requestId);
    if (future != null) {
      return future.complete(approved);
    }
    LOG.warn("No pending approval found for requestId={}", requestId);
    return false;
  }

  /**
   * Cancel all pending approval requests to unblock any waiting agent threads. Invoked as part of
   * engine shutdown via {@code ReactAgent.stop}.
   */
  @Override
  public void onStop() {
    InterruptedException ex = new InterruptedException("Session closed");
    pending.forEachKey(
        Long.MAX_VALUE,
        key -> {
          CompletableFuture<Boolean> future = pending.remove(key);
          if (future != null) {
            future.completeExceptionally(ex);
          }
        });
  }

  private static boolean shouldAutoApprove(ApprovalMode mode, ToolRiskLevel riskLevel) {
    if (mode == ApprovalMode.AUTO_APPROVE) {
      return true;
    }
    if (mode == ApprovalMode.NORMAL && riskLevel == ToolRiskLevel.SAFE) {
      return true;
    }
    // STRICT: all tools require approval
    return false;
  }
}
