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
import org.apache.kyuubi.engine.dataagent.datasource.JdbcDialect;
import org.apache.kyuubi.engine.dataagent.prompt.SystemPromptBuilder;
import org.apache.kyuubi.engine.dataagent.provider.DataAgentProvider;
import org.apache.kyuubi.engine.dataagent.provider.ProviderRunRequest;
import org.apache.kyuubi.engine.dataagent.runtime.AgentInvocation;
import org.apache.kyuubi.engine.dataagent.runtime.ConversationMemory;
import org.apache.kyuubi.engine.dataagent.runtime.ReactAgent;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentEvent;
import org.apache.kyuubi.engine.dataagent.runtime.middleware.ApprovalMiddleware;
import org.apache.kyuubi.engine.dataagent.runtime.middleware.CompactionMiddleware;
import org.apache.kyuubi.engine.dataagent.runtime.middleware.LoggingMiddleware;
import org.apache.kyuubi.engine.dataagent.runtime.middleware.ToolResultOffloadMiddleware;
import org.apache.kyuubi.engine.dataagent.tool.ToolRegistry;
import org.apache.kyuubi.engine.dataagent.tool.sql.RunMutationQueryTool;
import org.apache.kyuubi.engine.dataagent.tool.sql.RunSelectQueryTool;
import org.apache.kyuubi.engine.dataagent.util.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An OpenAI-compatible provider that wires up the full ReactAgent with streaming LLM, tools, and
 * middleware pipeline. Uses the official OpenAI Java SDK.
 *
 * <p>The ReactAgent, DataSource, and ToolRegistry are shared across all sessions within this engine
 * instance. Each session only maintains its own {@link ConversationMemory}. This works because each
 * engine is bound to one user + one datasource, so all sessions within the engine naturally share
 * the same data connection.
 */
public class OpenAiProvider implements DataAgentProvider {

  private static final Logger LOG = LoggerFactory.getLogger(OpenAiProvider.class);

  private final ReactAgent agent;
  private final ToolRegistry toolRegistry;
  private final DataSource dataSource;
  private final OpenAIClient client;
  private final ConcurrentHashMap<String, ConversationMemory> sessions = new ConcurrentHashMap<>();

  public OpenAiProvider(KyuubiConf conf) {
    String apiKey = ConfUtils.requireString(conf, KyuubiConf.ENGINE_DATA_AGENT_LLM_API_KEY());
    String baseUrl = ConfUtils.requireString(conf, KyuubiConf.ENGINE_DATA_AGENT_LLM_API_URL());
    String modelName = ConfUtils.requireString(conf, KyuubiConf.ENGINE_DATA_AGENT_LLM_MODEL());

    int maxIterations = ConfUtils.intConf(conf, KyuubiConf.ENGINE_DATA_AGENT_MAX_ITERATIONS());
    long compactionTriggerTokens =
        ConfUtils.longConf(conf, KyuubiConf.ENGINE_DATA_AGENT_COMPACTION_TRIGGER_TOKENS());
    int queryTimeoutSeconds =
        (int) ConfUtils.millisAsSeconds(conf, KyuubiConf.ENGINE_DATA_AGENT_QUERY_TIMEOUT());
    long toolCallTimeoutSeconds =
        ConfUtils.millisAsSeconds(conf, KyuubiConf.ENGINE_DATA_AGENT_TOOL_CALL_TIMEOUT());

    this.client =
        OpenAIOkHttpClient.builder()
            .apiKey(apiKey)
            .baseUrl(baseUrl)
            .maxRetries(3)
            .timeout(Duration.ofSeconds(180))
            .build();

    this.toolRegistry = new ToolRegistry(toolCallTimeoutSeconds);

    SystemPromptBuilder promptBuilder = SystemPromptBuilder.create();
    this.dataSource = attachJdbcDataSource(conf, toolRegistry, promptBuilder, queryTimeoutSeconds);

    this.agent =
        ReactAgent.builder()
            .client(client)
            .modelName(modelName)
            .toolRegistry(toolRegistry)
            .addMiddleware(new ToolResultOffloadMiddleware())
            .addMiddleware(new LoggingMiddleware())
            .addMiddleware(new CompactionMiddleware(client, modelName, compactionTriggerTokens))
            .addMiddleware(new ApprovalMiddleware())
            .maxIterations(maxIterations)
            .systemPrompt(promptBuilder.build())
            .build();
  }

  /**
   * Register JDBC-backed SQL tools if a JDBC URL is configured. Returns the created {@link
   * DataSource} so the provider can close it on shutdown, or {@code null} when no JDBC is wired.
   */
  private static DataSource attachJdbcDataSource(
      KyuubiConf conf,
      ToolRegistry registry,
      SystemPromptBuilder promptBuilder,
      int queryTimeoutSeconds) {
    String jdbcUrl = ConfUtils.optionalString(conf, KyuubiConf.ENGINE_DATA_AGENT_JDBC_URL());
    if (jdbcUrl == null) {
      return null;
    }
    LOG.info("Data Agent JDBC URL configured ({})", jdbcUrl.replaceAll("//.*@", "//<redacted>@"));

    String sessionUser =
        ConfUtils.optionalString(conf, KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY());

    DataSource ds = DataSourceFactory.create(jdbcUrl, sessionUser);
    registry.register(new RunSelectQueryTool(ds, queryTimeoutSeconds));
    registry.register(new RunMutationQueryTool(ds, queryTimeoutSeconds));
    promptBuilder.datasource(JdbcDialect.fromUrl(jdbcUrl).datasourceName());
    return ds;
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
      throw new IllegalStateException("No open Data Agent session for id=" + sessionId);
    }

    AgentInvocation invocation =
        new AgentInvocation(request.getQuestion())
            .modelName(request.getModelName())
            .approvalMode(request.getApprovalMode())
            .sessionId(sessionId);
    agent.run(invocation, memory, onEvent);
  }

  @Override
  public boolean resolveApproval(String requestId, boolean approved) {
    return agent.resolveApproval(requestId, approved);
  }

  @Override
  public void close(String sessionId) {
    sessions.remove(sessionId);
    agent.closeSession(sessionId);
    LOG.info("Closed Data Agent session {}", sessionId);
  }

  @Override
  public void stop() {
    agent.stop();
    toolRegistry.close();
    if (dataSource instanceof HikariDataSource) {
      ((HikariDataSource) dataSource).close();
      LOG.info("Closed Data Agent connection pool");
    }
    client.close();
  }
}
