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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kyuubi.engine.dataagent.runtime.AgentRunContext;
import org.apache.kyuubi.engine.dataagent.runtime.ApprovalMode;
import org.apache.kyuubi.engine.dataagent.runtime.ConversationMemory;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentEvent;
import org.apache.kyuubi.engine.dataagent.runtime.event.ApprovalRequest;
import org.apache.kyuubi.engine.dataagent.runtime.event.EventType;
import org.apache.kyuubi.engine.dataagent.tool.AgentTool;
import org.apache.kyuubi.engine.dataagent.tool.ToolContext;
import org.apache.kyuubi.engine.dataagent.tool.ToolRegistry;
import org.apache.kyuubi.engine.dataagent.tool.ToolRiskLevel;
import org.junit.Before;
import org.junit.Test;

public class ApprovalMiddlewareTest {

  private ToolRegistry registry;
  private List<AgentEvent> emittedEvents;

  @Before
  public void setUp() {
    registry = new ToolRegistry(30);
    registry.register(safeTool("safe_tool"));
    registry.register(destructiveTool("dangerous_tool"));
    emittedEvents = Collections.synchronizedList(new ArrayList<>());
  }

  // --- Auto-approve mode: all tools pass ---

  @Test
  public void testAutoApproveModeSkipsAllApproval() {
    ApprovalMiddleware mw = newApprovalMiddleware();
    AgentRunContext ctx = makeContext(ApprovalMode.AUTO_APPROVE);

    assertSame(
        AgentMiddleware.ToolCallApproval.INSTANCE,
        mw.beforeToolCall(ctx, "tc1", "dangerous_tool", Collections.emptyMap()));
    assertSame(
        AgentMiddleware.ToolCallApproval.INSTANCE,
        mw.beforeToolCall(ctx, "tc2", "safe_tool", Collections.emptyMap()));
    assertTrue("No approval events should be emitted", emittedEvents.isEmpty());
  }

  // --- Normal mode: safe auto-approved, destructive needs approval ---

  @Test
  public void testNormalModeAutoApprovesSafeTool() {
    ApprovalMiddleware mw = newApprovalMiddleware();
    AgentRunContext ctx = makeContext(ApprovalMode.NORMAL);

    assertSame(
        AgentMiddleware.ToolCallApproval.INSTANCE,
        mw.beforeToolCall(ctx, "tc1", "safe_tool", Collections.emptyMap()));
    assertTrue(emittedEvents.isEmpty());
  }

  @Test
  public void testNormalModeRequiresApprovalForDestructiveTool() throws Exception {
    ApprovalMiddleware mw = newApprovalMiddleware(5);
    AgentRunContext ctx = makeContext(ApprovalMode.NORMAL);

    ExecutorService exec = Executors.newSingleThreadExecutor();
    try {
      CountDownLatch eventEmitted = new CountDownLatch(1);
      // Capture the emitted event to get the requestId
      ctx.setEventEmitter(
          event -> {
            emittedEvents.add(event);
            eventEmitted.countDown();
          });

      Future<AgentMiddleware.ToolCallAction> future =
          exec.submit(
              () -> mw.beforeToolCall(ctx, "tc1", "dangerous_tool", Collections.emptyMap()));

      // Wait for the approval request event
      assertTrue("Approval event should be emitted", eventEmitted.await(2, TimeUnit.SECONDS));
      assertEquals(1, emittedEvents.size());
      assertEquals(EventType.APPROVAL_REQUEST, emittedEvents.get(0).eventType());

      ApprovalRequest req = (ApprovalRequest) emittedEvents.get(0);
      assertEquals("dangerous_tool", req.toolName());
      assertEquals(ToolRiskLevel.DESTRUCTIVE, req.riskLevel());

      // Approve
      assertTrue(mw.resolve(req.requestId(), true));
      assertSame(
          "Approved tool should return null (no denial)",
          AgentMiddleware.ToolCallApproval.INSTANCE,
          future.get(2, TimeUnit.SECONDS));
    } finally {
      exec.shutdownNow();
    }
  }

  @Test
  public void testDeniedToolReturnsToolCallDenial() throws Exception {
    ApprovalMiddleware mw = newApprovalMiddleware(5);
    AgentRunContext ctx = makeContext(ApprovalMode.NORMAL);

    ExecutorService exec = Executors.newSingleThreadExecutor();
    try {
      CountDownLatch eventEmitted = new CountDownLatch(1);
      ctx.setEventEmitter(
          event -> {
            emittedEvents.add(event);
            eventEmitted.countDown();
          });

      Future<AgentMiddleware.ToolCallAction> future =
          exec.submit(
              () -> mw.beforeToolCall(ctx, "tc1", "dangerous_tool", Collections.emptyMap()));

      assertTrue(eventEmitted.await(2, TimeUnit.SECONDS));
      ApprovalRequest req = (ApprovalRequest) emittedEvents.get(0);

      // Deny
      assertTrue(mw.resolve(req.requestId(), false));
      AgentMiddleware.ToolCallAction action = future.get(2, TimeUnit.SECONDS);
      assertTrue(action instanceof AgentMiddleware.ToolCallDenial);
      assertTrue(((AgentMiddleware.ToolCallDenial) action).reason().contains("denied"));
    } finally {
      exec.shutdownNow();
    }
  }

  // --- Strict mode: all tools need approval ---

  @Test
  public void testStrictModeRequiresApprovalForSafeTool() throws Exception {
    ApprovalMiddleware mw = newApprovalMiddleware(5);
    AgentRunContext ctx = makeContext(ApprovalMode.STRICT);

    ExecutorService exec = Executors.newSingleThreadExecutor();
    try {
      CountDownLatch eventEmitted = new CountDownLatch(1);
      ctx.setEventEmitter(
          event -> {
            emittedEvents.add(event);
            eventEmitted.countDown();
          });

      Future<AgentMiddleware.ToolCallAction> future =
          exec.submit(() -> mw.beforeToolCall(ctx, "tc1", "safe_tool", Collections.emptyMap()));

      assertTrue(eventEmitted.await(2, TimeUnit.SECONDS));
      ApprovalRequest req = (ApprovalRequest) emittedEvents.get(0);
      assertEquals("safe_tool", req.toolName());

      assertTrue(mw.resolve(req.requestId(), true));
      assertSame(AgentMiddleware.ToolCallApproval.INSTANCE, future.get(2, TimeUnit.SECONDS));
    } finally {
      exec.shutdownNow();
    }
  }

  // --- Timeout ---

  @Test
  public void testApprovalTimeoutReturnsDenial() throws Exception {
    ApprovalMiddleware mw = newApprovalMiddleware(1); // 1 second timeout
    AgentRunContext ctx = makeContext(ApprovalMode.STRICT);
    ctx.setEventEmitter(emittedEvents::add);

    ExecutorService exec = Executors.newSingleThreadExecutor();
    try {
      Future<AgentMiddleware.ToolCallAction> future =
          exec.submit(() -> mw.beforeToolCall(ctx, "tc1", "safe_tool", Collections.emptyMap()));

      // Don't resolve — let it time out
      AgentMiddleware.ToolCallAction action = future.get(5, TimeUnit.SECONDS);
      assertTrue(
          "Timeout should produce a denial", action instanceof AgentMiddleware.ToolCallDenial);
      assertTrue(((AgentMiddleware.ToolCallDenial) action).reason().contains("timed out"));
    } finally {
      exec.shutdownNow();
    }
  }

  // --- Cancel all ---

  @Test
  public void testOnStopUnblocksPendingRequests() throws Exception {
    ApprovalMiddleware mw = newApprovalMiddleware(30);
    AgentRunContext ctx = makeContext(ApprovalMode.STRICT);
    ctx.setEventEmitter(emittedEvents::add);

    ExecutorService exec = Executors.newSingleThreadExecutor();
    try {
      CountDownLatch started = new CountDownLatch(1);
      Future<AgentMiddleware.ToolCallAction> future =
          exec.submit(
              () -> {
                started.countDown();
                return mw.beforeToolCall(ctx, "tc1", "safe_tool", Collections.emptyMap());
              });

      assertTrue(started.await(2, TimeUnit.SECONDS));
      Thread.sleep(100); // let the thread enter the blocking wait

      mw.onStop();

      AgentMiddleware.ToolCallAction action = future.get(2, TimeUnit.SECONDS);
      assertTrue(
          "onStop should unblock with a denial", action instanceof AgentMiddleware.ToolCallDenial);
    } finally {
      exec.shutdownNow();
    }
  }

  // --- Helpers ---

  private ApprovalMiddleware newApprovalMiddleware() {
    ApprovalMiddleware mw = new ApprovalMiddleware();
    mw.onRegister(registry);
    return mw;
  }

  private ApprovalMiddleware newApprovalMiddleware(long timeoutSeconds) {
    ApprovalMiddleware mw = new ApprovalMiddleware(timeoutSeconds);
    mw.onRegister(registry);
    return mw;
  }

  private AgentRunContext makeContext(ApprovalMode mode) {
    AgentRunContext ctx = new AgentRunContext(new ConversationMemory(), mode);
    ctx.setEventEmitter(emittedEvents::add);
    return ctx;
  }

  private static AgentTool<DummyArgs> safeTool(String name) {
    return new DummyTool(name, ToolRiskLevel.SAFE);
  }

  private static AgentTool<DummyArgs> destructiveTool(String name) {
    return new DummyTool(name, ToolRiskLevel.DESTRUCTIVE);
  }

  public static class DummyArgs {
    public String value;
  }

  private static class DummyTool implements AgentTool<DummyArgs> {
    private final String name;
    private final ToolRiskLevel riskLevel;

    DummyTool(String name, ToolRiskLevel riskLevel) {
      this.name = name;
      this.riskLevel = riskLevel;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String description() {
      return "dummy tool";
    }

    @Override
    public ToolRiskLevel riskLevel() {
      return riskLevel;
    }

    @Override
    public Class<DummyArgs> argsType() {
      return DummyArgs.class;
    }

    @Override
    public String execute(DummyArgs args, ToolContext ctx) {
      return "ok";
    }
  }
}
