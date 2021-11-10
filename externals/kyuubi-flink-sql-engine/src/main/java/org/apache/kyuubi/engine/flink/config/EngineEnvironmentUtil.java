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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.JarUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for reading engine environment file. */
public class EngineEnvironmentUtil {
  private static final Logger LOG = LoggerFactory.getLogger(EngineEnvironmentUtil.class);

  public static EngineEnvironment readEnvironment(URL envUrl) {
    // use an empty environment by default
    if (envUrl == null) {
      System.out.println("No session environment specified.");
      return new EngineEnvironment();
    }

    System.out.println("Reading configuration from: " + envUrl);
    LOG.info("Using configuration file: {}", envUrl);
    try {
      return EngineEnvironment.parse(envUrl);
    } catch (IOException e) {
      throw new RuntimeException("Could not read configuration file at: " + envUrl, e);
    }
  }

  public static List<URL> discoverDependencies(List<URL> jars, List<URL> libraries) {
    final List<URL> dependencies = new ArrayList<>();
    try {
      // find jar files
      for (URL url : jars) {
        JarUtils.checkJarFile(url);
        dependencies.add(url);
      }

      // find jar files in library directories
      for (URL libUrl : libraries) {
        final File dir = new File(libUrl.toURI());
        if (!dir.isDirectory()) {
          throw new RuntimeException("Directory expected: " + dir);
        } else if (!dir.canRead()) {
          throw new RuntimeException("Directory cannot be read: " + dir);
        }
        final File[] files = dir.listFiles();
        if (files == null) {
          throw new RuntimeException("Directory cannot be read: " + dir);
        }
        for (File f : files) {
          // only consider jars
          if (f.isFile() && f.getAbsolutePath().toLowerCase().endsWith(".jar")) {
            final URL url = f.toURI().toURL();
            JarUtils.checkJarFile(url);
            dependencies.add(url);
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Could not load all required JAR files.", e);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Using the following dependencies: {}", dependencies);
    }

    return dependencies;
  }

  public static void checkFlinkVersion() {
    String flinkVersion = EnvironmentInformation.getVersion();
    if (!flinkVersion.startsWith("1.12")) {
      LOG.error("Only Flink-1.12 is supported now!");
      throw new RuntimeException("Only Flink-1.12 is supported now!");
    }
  }
}
