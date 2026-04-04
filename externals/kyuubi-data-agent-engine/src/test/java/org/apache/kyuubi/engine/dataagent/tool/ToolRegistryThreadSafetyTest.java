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

package org.apache.kyuubi.engine.dataagent.tool;

import static org.junit.Assert.*;

import com.openai.models.ChatModel;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/** Thread safety tests for ToolRegistry. Uses real tool implementations. */
public class ToolRegistryThreadSafetyTest {

  @Test
  public void testConcurrentRegisterAndAccess() throws Exception {
    ToolRegistry registry = new ToolRegistry();
    int numThreads = 8;
    CyclicBarrier barrier = new CyclicBarrier(numThreads);
    AtomicInteger errors = new AtomicInteger(0);
    ExecutorService pool = Executors.newFixedThreadPool(numThreads);

    // Half threads register, half threads read
    for (int i = 0; i < numThreads; i++) {
      final int idx = i;
      pool.submit(
          () -> {
            try {
              barrier.await();
              if (idx % 2 == 0) {
                // Register a tool
                registry.register(
                    new AgentTool<DummyArgs>() {
                      @Override
                      public String name() {
                        return "tool_" + idx;
                      }

                      @Override
                      public String description() {
                        return "test tool " + idx;
                      }

                      @Override
                      public Class<DummyArgs> argsType() {
                        return DummyArgs.class;
                      }

                      @Override
                      public String execute(DummyArgs args) {
                        return "result_" + idx;
                      }
                    });
              } else {
                // Read — may see partial state, but should not throw
                try {
                  registry.isEmpty();
                  ChatCompletionCreateParams.Builder builder =
                      ChatCompletionCreateParams.builder()
                          .model(ChatModel.GPT_4O)
                          .addUserMessage("test");
                  registry.addToolsTo(builder);
                } catch (Exception e) {
                  errors.incrementAndGet();
                }
              }
            } catch (Exception e) {
              errors.incrementAndGet();
            }
          });
    }

    pool.shutdown();
    assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
    assertEquals("Should have no errors from concurrent access", 0, errors.get());
    assertFalse(registry.isEmpty());
  }

  @Test
  public void testConcurrentExecuteWhileRegistering() throws Exception {
    ToolRegistry registry = new ToolRegistry();

    // Pre-register a tool
    registry.register(
        new AgentTool<DummyArgs>() {
          @Override
          public String name() {
            return "existing_tool";
          }

          @Override
          public String description() {
            return "existing";
          }

          @Override
          public Class<DummyArgs> argsType() {
            return DummyArgs.class;
          }

          @Override
          public String execute(DummyArgs args) {
            return "existing_result";
          }
        });

    int numThreads = 8;
    CyclicBarrier barrier = new CyclicBarrier(numThreads);
    AtomicInteger errors = new AtomicInteger(0);
    ExecutorService pool = Executors.newFixedThreadPool(numThreads);

    for (int i = 0; i < numThreads; i++) {
      final int idx = i;
      pool.submit(
          () -> {
            try {
              barrier.await();
              for (int j = 0; j < 100; j++) {
                if (idx % 2 == 0) {
                  String result = registry.executeTool("existing_tool", "{}");
                  if (!result.equals("existing_result")) {
                    errors.incrementAndGet();
                  }
                } else {
                  registry.register(
                      new AgentTool<DummyArgs>() {
                        @Override
                        public String name() {
                          return "dynamic_" + idx + "_" + Thread.currentThread().getId();
                        }

                        @Override
                        public String description() {
                          return "dynamic";
                        }

                        @Override
                        public Class<DummyArgs> argsType() {
                          return DummyArgs.class;
                        }

                        @Override
                        public String execute(DummyArgs args) {
                          return "dynamic";
                        }
                      });
                }
              }
            } catch (Exception e) {
              errors.incrementAndGet();
            }
          });
    }

    pool.shutdown();
    assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
    assertEquals(0, errors.get());
  }

  /** Minimal args class for testing. */
  public static class DummyArgs {}
}
