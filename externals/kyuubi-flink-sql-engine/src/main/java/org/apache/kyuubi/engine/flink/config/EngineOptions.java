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

package org.apache.kyuubi.engine.flink.config;

import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/** Engine options to configure the engine server. */
public class EngineOptions {
  private final boolean isPrintHelp;
  private final Integer port;
  private final URL defaultConfig;
  private final List<URL> jars;
  private final List<URL> libraryDirs;

  public EngineOptions(
      boolean isPrintHelp,
      @Nullable Integer port,
      @Nullable URL defaultConfig,
      @Nullable List<URL> jars,
      @Nullable List<URL> libraryDirs) {
    this.isPrintHelp = isPrintHelp;
    this.port = port;
    this.defaultConfig = defaultConfig;
    this.jars = jars != null ? jars : Collections.emptyList();
    this.libraryDirs = libraryDirs != null ? libraryDirs : Collections.emptyList();
  }

  public boolean isPrintHelp() {
    return isPrintHelp;
  }

  public Optional<Integer> getPort() {
    return Optional.ofNullable(port);
  }

  public Optional<URL> getDefaultConfig() {
    return Optional.ofNullable(defaultConfig);
  }

  public List<URL> getJars() {
    return jars;
  }

  public List<URL> getLibraryDirs() {
    return libraryDirs;
  }
}
