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

package org.apache.kyuubi.engine.dataagent.provider.openai;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.apache.kyuubi.config.KyuubiConf;
import org.apache.kyuubi.config.KyuubiReservedKeys;
import org.apache.kyuubi.engine.dataagent.datasource.DataSourceFactory;
import org.apache.kyuubi.engine.dataagent.prompt.SystemPromptBuilder;
import org.apache.kyuubi.engine.dataagent.provider.DataAgentProvider;
import org.apache.kyuubi.engine.dataagent.provider.ProviderRunRequest;
import org.apache.kyuubi.engine.dataagent.runtime.AgentRunRequest;
import org.apache.kyuubi.engine.dataagent.runtime.ApprovalMode;
import org.apache.kyuubi.engine.dataagent.runtime.ConversationMemory;
import org.apache.kyuubi.engine.dataagent.runtime.ReactAgent;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentError;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentEvent;
import org.apache.kyuubi.engine.dataagent.runtime.middleware.ApprovalMiddleware;
import org.apache.kyuubi.engine.dataagent.runtime.middleware.LoggingMiddleware;
import org.apache.kyuubi.engine.dataagent.tool.ToolRegistry;
import org.apache.kyuubi.engine.dataagent.tool.sql.SqlQueryTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An OpenAI-compatible provider that wires up the full ReactAgent with streaming LLM, tools, and
 * middleware pipeline. Uses the official OpenAI Java SDK.
 *
 * <p>The ReactAgent, DataSource, and ToolRegistry are shared across all sessions within this engine
 * instance. Each session only maintains its own {@link ConversationMemory}. This works because each
 * engine is bound to one user + one datasource (via USER share level + subdomain isolation), so all
 * sessions within the engine naturally share the same data connection.
 */
public class OpenAiProvider implements DataAgentProvider {

  private static final Logger LOG = LoggerFactory.getLogger(OpenAiProvider.class);

  private final ReactAgent agent;
  private final ApprovalMiddleware approvalMiddleware;
  private final DataSource dataSource;
  private final ConcurrentHashMap<String, ConversationMemory> sessions = new ConcurrentHashMap<>();

  public OpenAiProvider(KyuubiConf conf) {
    scala.Option<String> apiKeyOpt = conf.get(KyuubiConf.ENGINE_DATA_AGENT_LLM_API_KEY());
    if (apiKeyOpt.isEmpty()) {
      throw new IllegalArgumentException(
          KyuubiConf.ENGINE_DATA_AGENT_LLM_API_KEY().key()
              + " is required for OPENAI_COMPATIBLE provider");
    }
    scala.Option<String> apiUrlOpt = conf.get(KyuubiConf.ENGINE_DATA_AGENT_LLM_API_URL());
    if (apiUrlOpt.isEmpty()) {
      throw new IllegalArgumentException(
          KyuubiConf.ENGINE_DATA_AGENT_LLM_API_URL().key()
              + " is required for OPENAI_COMPATIBLE provider");
    }
    String apiKey = apiKeyOpt.get();
    String baseUrl = apiUrlOpt.get();
    String modelName = conf.get(KyuubiConf.ENGINE_DATA_AGENT_LLM_MODEL());

    OpenAIClient client =
        OpenAIOkHttpClient.builder()
            .apiKey(apiKey)
            .baseUrl(baseUrl)
            .maxRetries(3)
            .timeout(Duration.ofSeconds(120))
            .build();

    int maxIterations = (int) conf.get(KyuubiConf.ENGINE_DATA_AGENT_MAX_ITERATIONS());
    int queryTimeoutSeconds = (int) conf.get(KyuubiConf.ENGINE_DATA_AGENT_QUERY_TIMEOUT());

    // Register tools and build prompt from JDBC URL
    DataSource ds = null;
    ReactAgent builtAgent = null;
    try {
      ToolRegistry toolRegistry = new ToolRegistry();
      SystemPromptBuilder promptBuilder = SystemPromptBuilder.create();
      scala.Option<String> jdbcUrlOpt = conf.get(KyuubiConf.ENGINE_DATA_AGENT_JDBC_URL());
      if (jdbcUrlOpt.isDefined()) {
        String jdbcUrl = jdbcUrlOpt.get();
        LOG.info(
            "Data Agent JDBC URL configured ({})", jdbcUrl.replaceAll("//.*@", "//<redacted>@"));
        scala.Option<String> userOpt = conf.getOption(KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY());
        String sessionUser = userOpt.isDefined() ? userOpt.get() : null;
        ds = DataSourceFactory.create(jdbcUrl, sessionUser);
        toolRegistry.register(new SqlQueryTool(ds, queryTimeoutSeconds));
        promptBuilder.jdbcUrl(jdbcUrl);
      }

      ApprovalMiddleware approval = new ApprovalMiddleware(toolRegistry);

      builtAgent =
          ReactAgent.builder()
              .client(client)
              .modelName(modelName)
              .toolRegistry(toolRegistry)
              .addMiddleware(new LoggingMiddleware())
              .addMiddleware(approval)
              .maxIterations(maxIterations)
              .toolTimeoutSeconds(queryTimeoutSeconds)
              .systemPrompt(promptBuilder.build())
              .build();

      this.agent = builtAgent;
      this.approvalMiddleware = approval;
      this.dataSource = ds;
    } catch (Exception e) {
      if (builtAgent != null) {
        try {
          builtAgent.close();
        } catch (Exception ex) {
          LOG.warn("Error closing ReactAgent during constructor cleanup", ex);
        }
      }
      if (ds instanceof HikariDataSource) {
        ((HikariDataSource) ds).close();
      }
      throw e;
    }
  }

  @Override
  public void open(String sessionId, String user) {
    sessions.put(sessionId, new ConversationMemory());
    LOG.info("Opened Data Agent session {} for user {}", sessionId, user);
  }

  @Override
  public void run(String sessionId, ProviderRunRequest request, Consumer<AgentEvent> onEvent) {
    ConversationMemory memory = sessions.get(sessionId);
    if (memory == null) {
      onEvent.accept(new AgentError("Session not found. Please reconnect."));
      return;
    }

    AgentRunRequest agentRequest =
        new AgentRunRequest(request.getQuestion()).modelName(request.getModelName());
    String modeStr = request.getApprovalMode();
    if (modeStr != null && !modeStr.isEmpty()) {
      try {
        agentRequest.approvalMode(ApprovalMode.valueOf(modeStr.toUpperCase()));
      } catch (IllegalArgumentException e) {
        LOG.warn("Unknown approval mode '{}', using default NORMAL", modeStr);
      }
    }
    agent.run(agentRequest, memory, onEvent);
  }

  @Override
  public boolean resolveApproval(String requestId, boolean approved) {
    return approvalMiddleware.resolve(requestId, approved);
  }

  @Override
  public void close(String sessionId) {
    sessions.remove(sessionId);
    LOG.info("Closed Data Agent session {}", sessionId);
  }

  @Override
  public void stop() {
    approvalMiddleware.cancelAll();
    try {
      agent.close();
    } catch (Exception e) {
      LOG.warn("Error closing ReactAgent", e);
    }
    if (dataSource instanceof HikariDataSource) {
      ((HikariDataSource) dataSource).close();
      LOG.info("Closed Data Agent connection pool");
    }
  }
}
