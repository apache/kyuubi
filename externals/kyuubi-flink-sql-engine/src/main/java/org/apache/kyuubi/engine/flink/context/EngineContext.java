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

import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.commons.cli.Options;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.kyuubi.engine.flink.config.EngineEnvironment;

/** Context describing default environment, dependencies, flink config, etc. */
public class EngineContext {
  private final EngineEnvironment engineEnv;
  private final List<URL> dependencies;
  private final Configuration flinkConfig;
  private final List<CustomCommandLine> commandLines;
  private final Options commandLineOptions;
  private final ClusterClientServiceLoader clusterClientServiceLoader;

  public EngineContext(EngineEnvironment engineEnv, List<URL> dependencies) {
    this.engineEnv = engineEnv;
    this.dependencies = dependencies;

    // discover configuration
    final String flinkConfigDir;
    try {
      // find the configuration directory
      flinkConfigDir = CliFrontend.getConfigurationDirectoryFromEnv();

      // load the global configuration
      this.flinkConfig = GlobalConfiguration.loadConfiguration(flinkConfigDir);

      // initialize default file system
      FileSystem.initialize(
          flinkConfig, PluginUtils.createPluginManagerFromRootFolder(flinkConfig));

      // load command lines for deployment
      this.commandLines = CliFrontend.loadCustomCommandLines(flinkConfig, flinkConfigDir);
      this.commandLineOptions = collectCommandLineOptions(commandLines);
    } catch (Exception e) {
      throw new RuntimeException("Could not load Flink configuration.", e);
    }
    clusterClientServiceLoader = new DefaultClusterClientServiceLoader();
  }

  /** Constructor for testing purposes. */
  @VisibleForTesting
  public EngineContext(
      EngineEnvironment engineEnv,
      List<URL> dependencies,
      Configuration flinkConfig,
      CustomCommandLine commandLine,
      ClusterClientServiceLoader clusterClientServiceLoader) {
    this.engineEnv = engineEnv;
    this.dependencies = dependencies;
    this.flinkConfig = flinkConfig;
    this.commandLines = Collections.singletonList(commandLine);
    this.commandLineOptions = collectCommandLineOptions(commandLines);
    this.clusterClientServiceLoader = Objects.requireNonNull(clusterClientServiceLoader);
  }

  public Configuration getFlinkConfig() {
    return flinkConfig;
  }

  public EngineEnvironment getEngineEnv() {
    return engineEnv;
  }

  public List<URL> getDependencies() {
    return dependencies;
  }

  public List<CustomCommandLine> getCommandLines() {
    return commandLines;
  }

  public Options getCommandLineOptions() {
    return commandLineOptions;
  }

  public ClusterClientServiceLoader getClusterClientServiceLoader() {
    return clusterClientServiceLoader;
  }

  private Options collectCommandLineOptions(List<CustomCommandLine> commandLines) {
    final Options customOptions = new Options();
    for (CustomCommandLine customCommandLine : commandLines) {
      customCommandLine.addGeneralOptions(customOptions);
      customCommandLine.addRunOptions(customOptions);
    }
    return CliFrontendParser.mergeOptions(CliFrontendParser.getRunCommandOptions(), customOptions);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof EngineContext)) {
      return false;
    }
    EngineContext context = (EngineContext) o;
    return Objects.equals(engineEnv, context.engineEnv)
        && Objects.equals(dependencies, context.dependencies)
        && Objects.equals(flinkConfig, context.flinkConfig)
        && Objects.equals(commandLines, context.commandLines)
        && Objects.equals(commandLineOptions, context.commandLineOptions)
        && Objects.equals(clusterClientServiceLoader, context.clusterClientServiceLoader);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        engineEnv,
        dependencies,
        flinkConfig,
        commandLines,
        commandLineOptions,
        clusterClientServiceLoader);
  }
}
