/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.engine.flink.context;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.cli.ProgramOptions;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.BatchTableEnvironmentImpl;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.ComponentFactoryService;
import org.apache.flink.table.factories.ModuleFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.apache.kyuubi.engine.flink.config.EngineEnvironment;
import org.apache.kyuubi.engine.flink.config.entries.DeploymentEntry;
import org.apache.kyuubi.engine.flink.config.entries.ExecutionEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Context for executing table programs. This class caches everything that can be cached across
 * multiple queries as long as the session context does not change. This must be thread-safe as it
 * might be reused across different query submissions.
 *
 * @param <ClusterID> cluster id
 */
public class ExecutionContext<ClusterID> {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionContext.class);

  private final EngineEnvironment engineEnvironment;
  private final ClassLoader classLoader;

  private final Configuration flinkConfig;
//  private final ClusterClientFactory<ClusterID> clusterClientFactory;

  private TableEnvironmentInternal tableEnv;
  private ExecutionEnvironment execEnv;
  private StreamExecutionEnvironment streamExecEnv;
  private Executor executor;

  // Members that should be reused in the same session.
  private SessionState sessionState;

  private ExecutionContext(
      EngineEnvironment engineEnvironment,
      @Nullable SessionState sessionState,
      List<URL> dependencies,
      Configuration flinkConfig,
      ClusterClientServiceLoader clusterClientServiceLoader,
      Options commandLineOptions,
      List<CustomCommandLine> availableCommandLines)
      throws FlinkException {
    this.engineEnvironment = engineEnvironment;

    this.flinkConfig = flinkConfig;

    // create class loader
    classLoader =
        ClientUtils.buildUserCodeClassLoader(
            dependencies, Collections.emptyList(), this.getClass().getClassLoader(), flinkConfig);

    // Initialize the TableEnvironment.
    initializeTableEnvironment(sessionState);
  }

  /**
   * Executes the given supplier using the execution context's classloader as thread classloader.
   */
  public <R> R wrapClassLoader(Supplier<R> supplier) {
    try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
      return supplier.get();
    }
  }

  /**
   * Executes the given Runnable using the execution context's classloader as thread classloader.
   */
  void wrapClassLoader(Runnable runnable) {
    try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
      runnable.run();
    }
  }

  public TableEnvironmentInternal getTableEnvironment() {
    return tableEnv;
  }

  /** Returns a builder for this {@link ExecutionContext}. */
  public static Builder builder(
      EngineEnvironment defaultEnv,
      EngineEnvironment sessionEnv,
      List<URL> dependencies,
      Configuration configuration,
      ClusterClientServiceLoader serviceLoader,
      Options commandLineOptions,
      List<CustomCommandLine> commandLines) {
    return new Builder(
        defaultEnv,
        sessionEnv,
        dependencies,
        configuration,
        serviceLoader,
        commandLineOptions,
        commandLines);
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Non-public methods
  // ------------------------------------------------------------------------------------------------------------------

  private static Configuration createExecutionConfig(
      CommandLine commandLine,
      Options commandLineOptions,
      List<CustomCommandLine> availableCommandLines,
      List<URL> dependencies)
      throws FlinkException {
    LOG.debug("Available commandline options: {}", commandLineOptions);
    List<String> options =
        Stream.of(commandLine.getOptions())
            .map(o -> o.getOpt() + "=" + o.getValue())
            .collect(Collectors.toList());
    LOG.debug("Instantiated commandline args: {}, options: {}", commandLine.getArgList(), options);

    final CustomCommandLine activeCommandLine =
        findActiveCommandLine(availableCommandLines, commandLine);
    LOG.debug(
        "Available commandlines: {}, active commandline: {}",
        availableCommandLines,
        activeCommandLine);

    Configuration executionConfig = activeCommandLine.toConfiguration(commandLine);

    try {
      final ProgramOptions programOptions = ProgramOptions.create(commandLine);
      final ExecutionConfigAccessor executionConfigAccessor =
          ExecutionConfigAccessor.fromProgramOptions(programOptions, dependencies);
      executionConfigAccessor.applyToConfiguration(executionConfig);
    } catch (CliArgsException e) {
      throw new RuntimeException("Invalid deployment run options.", e);
    }

    LOG.info("Executor config: {}", executionConfig);
    return executionConfig;
  }

  private static CustomCommandLine findActiveCommandLine(
      List<CustomCommandLine> availableCommandLines, CommandLine commandLine) {
    for (CustomCommandLine cli : availableCommandLines) {
      if (cli.isActive(commandLine)) {
        return cli;
      }
    }
    throw new RuntimeException("Could not find a matching deployment.");
  }

  private Module createModule(Map<String, String> moduleProperties, ClassLoader classLoader) {
    final ModuleFactory factory =
        TableFactoryService.find(ModuleFactory.class, moduleProperties, classLoader);
    return factory.createModule(moduleProperties);
  }

  private Catalog createCatalog(
      String name, Map<String, String> catalogProperties, ClassLoader classLoader) {
    final CatalogFactory factory =
        TableFactoryService.find(CatalogFactory.class, catalogProperties, classLoader);
    return factory.createCatalog(name, catalogProperties);
  }

  private static TableSource<?> createTableSource(
      ExecutionEntry execution, Map<String, String> sourceProperties, ClassLoader classLoader) {
    if (execution.isStreamingPlanner()) {
      final TableSourceFactory<?> factory =
          (TableSourceFactory<?>)
              TableFactoryService.find(TableSourceFactory.class, sourceProperties, classLoader);
      return factory.createTableSource(sourceProperties);
    } else if (execution.isBatchPlanner()) {
      final BatchTableSourceFactory<?> factory =
          (BatchTableSourceFactory<?>)
              TableFactoryService.find(
                  BatchTableSourceFactory.class, sourceProperties, classLoader);
      return factory.createBatchTableSource(sourceProperties);
    }
    throw new RuntimeException("Unsupported execution type for sources.");
  }

  private static TableSink<?> createTableSink(
      ExecutionEntry execution, Map<String, String> sinkProperties, ClassLoader classLoader) {
    if (execution.isStreamingPlanner()) {
      final TableSinkFactory<?> factory =
          (TableSinkFactory<?>)
              TableFactoryService.find(TableSinkFactory.class, sinkProperties, classLoader);
      return factory.createTableSink(sinkProperties);
    } else if (execution.isBatchPlanner()) {
      final BatchTableSinkFactory<?> factory =
          (BatchTableSinkFactory<?>)
              TableFactoryService.find(BatchTableSinkFactory.class, sinkProperties, classLoader);
      return factory.createBatchTableSink(sinkProperties);
    }
    throw new RuntimeException("Unsupported execution type for sinks.");
  }

  private TableEnvironmentInternal createStreamTableEnvironment(
      StreamExecutionEnvironment env,
      EnvironmentSettings settings,
      TableConfig config,
      Executor executor,
      CatalogManager catalogManager,
      ModuleManager moduleManager,
      FunctionCatalog functionCatalog) {
    final Map<String, String> plannerProperties = settings.toPlannerProperties();
    final Planner planner =
        ComponentFactoryService.find(PlannerFactory.class, plannerProperties)
            .create(plannerProperties, executor, config, functionCatalog, catalogManager);

    return new StreamTableEnvironmentImpl(
        catalogManager,
        moduleManager,
        functionCatalog,
        config,
        env,
        planner,
        executor,
        settings.isStreamingMode(),
        classLoader);
  }

  private static Executor lookupExecutor(
      Map<String, String> executorProperties, StreamExecutionEnvironment executionEnvironment) {
    try {
      ExecutorFactory executorFactory =
          ComponentFactoryService.find(ExecutorFactory.class, executorProperties);
      Method createMethod =
          executorFactory
              .getClass()
              .getMethod("create", Map.class, StreamExecutionEnvironment.class);

      return (Executor)
          createMethod.invoke(executorFactory, executorProperties, executionEnvironment);
    } catch (Exception e) {
      throw new TableException(
          "Could not instantiate the executor. Make sure a planner module is on the classpath", e);
    }
  }

  private void initializeTableEnvironment(@Nullable SessionState sessionState) {
    final EnvironmentSettings settings = engineEnvironment.getExecution().getEnvironmentSettings();
    // Step 0.0 Initialize the table configuration.
    final TableConfig config = new TableConfig();
    engineEnvironment
        .getConfiguration()
        .asMap()
        .forEach((k, v) -> config.getConfiguration().setString(k, v));
    final boolean noInheritedState = sessionState == null;
    if (noInheritedState) {
      // --------------------------------------------------------------------------------------------------------------
      // Step.1 Create environments
      // --------------------------------------------------------------------------------------------------------------
      // Step 1.0 Initialize the ModuleManager if required.
      final ModuleManager moduleManager = new ModuleManager();
      // Step 1.1 Initialize the CatalogManager if required.
      final CatalogManager catalogManager =
          CatalogManager.newBuilder()
              .classLoader(classLoader)
              .config(config.getConfiguration())
              .defaultCatalog(
                  settings.getBuiltInCatalogName(),
                  new GenericInMemoryCatalog(
                      settings.getBuiltInCatalogName(), settings.getBuiltInDatabaseName()))
              .build();
      // Step 1.2 Initialize the FunctionCatalog if required.
      final FunctionCatalog functionCatalog =
          new FunctionCatalog(config, catalogManager, moduleManager);
      // Step 1.4 Set up session state.
      this.sessionState = SessionState.of(catalogManager, moduleManager, functionCatalog);

      // Must initialize the table engineEnvironment before actually the
      createTableEnvironment(settings, config, catalogManager, moduleManager, functionCatalog);

      // --------------------------------------------------------------------------------------------------------------
      // Step.4 Create catalogs and register them.
      // --------------------------------------------------------------------------------------------------------------
      // No need to register the catalogs if already inherit from the same session.
      initializeCatalogs();
    } else {
      // Set up session state.
      this.sessionState = sessionState;
      createTableEnvironment(
          settings,
          config,
          sessionState.catalogManager,
          sessionState.moduleManager,
          sessionState.functionCatalog);
    }
  }

  private void createTableEnvironment(
      EnvironmentSettings settings,
      TableConfig config,
      CatalogManager catalogManager,
      ModuleManager moduleManager,
      FunctionCatalog functionCatalog) {
    if (engineEnvironment.getExecution().isStreamingPlanner()) {
      streamExecEnv = createStreamExecutionEnvironment();
      execEnv = null;

      final Map<String, String> executorProperties = settings.toExecutorProperties();
      executor = lookupExecutor(executorProperties, streamExecEnv);
      tableEnv =
          createStreamTableEnvironment(
              streamExecEnv,
              settings,
              config,
              executor,
              catalogManager,
              moduleManager,
              functionCatalog);
    } else if (engineEnvironment.getExecution().isBatchPlanner()) {
      streamExecEnv = null;
      execEnv = createExecutionEnvironment();
      executor = null;
      tableEnv = new BatchTableEnvironmentImpl(execEnv, config, catalogManager, moduleManager);
    } else {
      throw new RuntimeException("Unsupported execution type specified.");
    }
  }

  private void initializeCatalogs() {
    // --------------------------------------------------------------------------------------------------------------
    // Step.1 Create catalogs and register them.
    // --------------------------------------------------------------------------------------------------------------
    wrapClassLoader(
        () -> {
          engineEnvironment
              .getCatalogs()
              .forEach(
                  (name, entry) -> {
                    Catalog catalog = createCatalog(name, entry.asMap(), classLoader);
                    tableEnv.registerCatalog(name, catalog);
                  });
        });

    // --------------------------------------------------------------------------------------------------------------
    // Step.6 Set current catalog and database.
    // --------------------------------------------------------------------------------------------------------------
    // Switch to the current catalog.
    Optional<String> catalog = engineEnvironment.getExecution().getCurrentCatalog();
    catalog.ifPresent(tableEnv::useCatalog);

    // Switch to the current database.
    Optional<String> database = engineEnvironment.getExecution().getCurrentDatabase();
    database.ifPresent(tableEnv::useDatabase);
  }

  private ExecutionEnvironment createExecutionEnvironment() {
    final ExecutionEnvironment execEnv = ExecutionEnvironment.getExecutionEnvironment();
    execEnv.setRestartStrategy(engineEnvironment.getExecution().getRestartStrategy());
    execEnv.setParallelism(engineEnvironment.getExecution().getParallelism());
    return execEnv;
  }

  private StreamExecutionEnvironment createStreamExecutionEnvironment() {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRestartStrategy(engineEnvironment.getExecution().getRestartStrategy());
    env.setParallelism(engineEnvironment.getExecution().getParallelism());
    env.setMaxParallelism(engineEnvironment.getExecution().getMaxParallelism());
    env.setStreamTimeCharacteristic(engineEnvironment.getExecution().getTimeCharacteristic());
    if (env.getStreamTimeCharacteristic() == TimeCharacteristic.EventTime) {
      env.getConfig()
          .setAutoWatermarkInterval(
              engineEnvironment.getExecution().getPeriodicWatermarksInterval());
    }
    return env;
  }

  private void registerFunctions(Map<String, FunctionDefinition> functions) {
    if (tableEnv instanceof StreamTableEnvironment) {
      StreamTableEnvironment streamTableEnvironment = (StreamTableEnvironment) tableEnv;
      functions.forEach(
          (k, v) -> {
            if (v instanceof ScalarFunction) {
              streamTableEnvironment.registerFunction(k, (ScalarFunction) v);
            } else if (v instanceof AggregateFunction) {
              streamTableEnvironment.registerFunction(k, (AggregateFunction<?, ?>) v);
            } else if (v instanceof TableFunction) {
              streamTableEnvironment.registerFunction(k, (TableFunction<?>) v);
            } else {
              throw new RuntimeException("Unsupported function type: " + v.getClass().getName());
            }
          });
    } else {
      BatchTableEnvironment batchTableEnvironment = (BatchTableEnvironment) tableEnv;
      functions.forEach(
          (k, v) -> {
            if (v instanceof ScalarFunction) {
              batchTableEnvironment.registerFunction(k, (ScalarFunction) v);
            } else if (v instanceof AggregateFunction) {
              batchTableEnvironment.registerFunction(k, (AggregateFunction<?, ?>) v);
            } else if (v instanceof TableFunction) {
              batchTableEnvironment.registerFunction(k, (TableFunction<?>) v);
            } else {
              throw new RuntimeException("Unsupported function type: " + v.getClass().getName());
            }
          });
    }
  }

  // ~ Inner Class -------------------------------------------------------------------------------

  /** Builder for {@link ExecutionContext}. */
  public static class Builder {
    // Required members.
    private final EngineEnvironment sessionEnv;
    private final List<URL> dependencies;
    private final Configuration configuration;
    private final ClusterClientServiceLoader serviceLoader;
    private final Options commandLineOptions;
    private final List<CustomCommandLine> commandLines;

    private EngineEnvironment defaultEnv;
    private EngineEnvironment currentEnv;

    // Optional members.
    @Nullable private SessionState sessionState;

    private Builder(
        EngineEnvironment defaultEnv,
        @Nullable EngineEnvironment sessionEnv,
        List<URL> dependencies,
        Configuration configuration,
        ClusterClientServiceLoader serviceLoader,
        Options commandLineOptions,
        List<CustomCommandLine> commandLines) {
      this.defaultEnv = defaultEnv;
      this.sessionEnv = sessionEnv;
      this.dependencies = dependencies;
      this.configuration = configuration;
      this.serviceLoader = serviceLoader;
      this.commandLineOptions = commandLineOptions;
      this.commandLines = commandLines;
    }

    public Builder env(EngineEnvironment engineEnvironment) {
      this.currentEnv = engineEnvironment;
      return this;
    }

    public Builder sessionState(SessionState sessionState) {
      this.sessionState = sessionState;
      return this;
    }

    public ExecutionContext<?> build() {
      try {
        return new ExecutionContext<>(
            this.currentEnv == null
                ? EngineEnvironment.merge(defaultEnv, sessionEnv)
                : this.currentEnv,
            this.sessionState,
            this.dependencies,
            this.configuration,
            this.serviceLoader,
            this.commandLineOptions,
            this.commandLines);
      } catch (Throwable t) {
        // catch everything such that a configuration does not crash the executor
        throw new RuntimeException("Could not create execution context.", t);
      }
    }
  }

  /** Represents the state that should be reused in one session. * */
  public static class SessionState {
    public final CatalogManager catalogManager;
    public final ModuleManager moduleManager;
    public final FunctionCatalog functionCatalog;

    private SessionState(
        CatalogManager catalogManager,
        ModuleManager moduleManager,
        FunctionCatalog functionCatalog) {
      this.catalogManager = catalogManager;
      this.moduleManager = moduleManager;
      this.functionCatalog = functionCatalog;
    }

    public static SessionState of(
        CatalogManager catalogManager,
        ModuleManager moduleManager,
        FunctionCatalog functionCatalog) {
      return new SessionState(catalogManager, moduleManager, functionCatalog);
    }
  }
}
