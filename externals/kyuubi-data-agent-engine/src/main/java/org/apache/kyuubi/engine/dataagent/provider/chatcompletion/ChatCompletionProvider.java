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

package org.apache.kyuubi.engine.dataagent.provider.chatcompletion;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.util.Map;
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
import org.apache.kyuubi.jdbc.hive.JdbcConnectionParams;
import org.apache.kyuubi.jdbc.hive.Utils;
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
public class ChatCompletionProvider implements DataAgentProvider {

  private static final Logger LOG = LoggerFactory.getLogger(ChatCompletionProvider.class);

  private final ReactAgent agent;
  private final ToolRegistry toolRegistry;
  private final DataSource dataSource;
  private final OpenAIClient client;
  private final ConcurrentHashMap<String, ConversationMemory> sessions = new ConcurrentHashMap<>();

  public ChatCompletionProvider(KyuubiConf conf) {
    String apiKey = ConfUtils.requireString(conf, KyuubiConf.ENGINE_DATA_AGENT_OPENAI_API_KEY());
    String baseUrl = ConfUtils.requireString(conf, KyuubiConf.ENGINE_DATA_AGENT_OPENAI_ENDPOINT());
    String modelName = ConfUtils.requireString(conf, KyuubiConf.ENGINE_DATA_AGENT_MODEL());

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
    JdbcDialect dialect = JdbcDialect.fromUrl(jdbcUrl);
    if (dialect == null) {
      throw new IllegalArgumentException(
          "Invalid " + KyuubiConf.ENGINE_DATA_AGENT_JDBC_URL().key());
    }
    LOG.info("Data Agent JDBC datasource configured ({})", dialect.datasourceName());

    // For Kyuubi/HiveServer2 backends, pass the session user for proxy-user impersonation —
    // but only when the URL does not already carry explicit credentials, because URL-supplied
    // credentials should win (some users pass credentials via session variables).
    String subprotocol = JdbcDialect.extractSubprotocol(jdbcUrl);
    boolean isKyuubiBackend = "kyuubi".equals(subprotocol) || "hive2".equals(subprotocol);
    // TODO: temporary ban. A Kerberized engine runs co-located with the Kyuubi server and sees its
    //  proxy super-user TGT, so passing the session user as proxy below could impersonate arbitrary
    //  users. Lift once the engine performs its own Kerberos login and proxies via proxyuser ACLs.
    if (isKyuubiBackend && resolvesToKerberos(jdbcUrl)) {
      throw new IllegalArgumentException(
          "Kerberos authentication is not yet supported for the Data Agent JDBC datasource. "
              + "The engine runs with the Kyuubi server's credentials, which could allow "
              + "impersonating arbitrary users. Use a non-Kerberos auth for "
              + KyuubiConf.ENGINE_DATA_AGENT_JDBC_URL().key()
              + " for now.");
    }
    String hikariUser = null;
    if (isKyuubiBackend && !hasKyuubiUrlCredentials(jdbcUrl)) {
      hikariUser = ConfUtils.optionalString(conf, KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY());
    }

    DataSource ds = DataSourceFactory.create(jdbcUrl, hikariUser);
    registry.register(new RunSelectQueryTool(ds, queryTimeoutSeconds));
    registry.register(new RunMutationQueryTool(ds, queryTimeoutSeconds));
    promptBuilder.datasource(dialect.datasourceName());
    return ds;
  }

  /**
   * Return true when a Kyuubi/HiveServer2 JDBC URL carries an explicit user identity as the
   * semicolon-delimited {@code ;user=...} session variable. Only semicolon parameters are checked
   * because the Kyuubi/Hive JDBC driver reads them as auth session vars — query parameters ({@code
   * ?key=val}) are parsed as Hive configuration, not authentication. A URL that only carries {@code
   * ;password=} without a user is not considered credentialed, so the session user is still passed
   * for proxy-user impersonation.
   */
  static boolean hasKyuubiUrlCredentials(String jdbcUrl) {
    if (jdbcUrl == null) {
      return false;
    }
    String lower = jdbcUrl.toLowerCase();
    return lower.contains(";user=");
  }

  /**
   * Return true when a Kyuubi/HiveServer2 JDBC URL resolves to Kerberos authentication. The URL is
   * parsed with the JDBC driver's own resolver rather than scanned as text, so a Kerberos principal
   * is detected whether it is written explicitly ({@code ;principal=}/{@code
   * ;kyuubiServerPrincipal=}) or injected during ZooKeeper service discovery — a plain {@code
   * ;serviceDiscoveryMode=zooKeeper} URL pointing at a Kerberized server carries no principal in
   * its text but the driver reads {@code hive.server2.authentication.kerberos.principal} from the
   * discovered ZooKeeper node. Resolving a discovery URL therefore contacts ZooKeeper. Fails
   * closed: if the URL cannot be resolved we cannot prove it is non-Kerberos, so this throws.
   */
  static boolean resolvesToKerberos(String jdbcUrl) {
    if (jdbcUrl == null) {
      return false;
    }
    final Map<String, String> sessionVars;
    try {
      sessionVars = Utils.parseURL(jdbcUrl).getSessionVars();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Could not resolve "
              + KyuubiConf.ENGINE_DATA_AGENT_JDBC_URL().key()
              + " to verify it does not use Kerberos authentication: "
              + e.getMessage(),
          e);
    }
    return sessionVars.containsKey(JdbcConnectionParams.AUTH_PRINCIPAL)
        || sessionVars.containsKey(JdbcConnectionParams.AUTH_KYUUBI_SERVER_PRINCIPAL)
        || "KERBEROS".equalsIgnoreCase(sessionVars.get(JdbcConnectionParams.AUTH_TYPE));
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
