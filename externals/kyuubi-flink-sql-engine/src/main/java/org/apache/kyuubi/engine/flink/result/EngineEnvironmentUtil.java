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

package org.apache.kyuubi.engine.flink.result;

import java.util.Arrays;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.cli.CliOptions;
import org.apache.flink.table.client.cli.CliOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for reading engine environment file. */
public class EngineEnvironmentUtil {

  private static final Logger LOG = LoggerFactory.getLogger(EngineEnvironmentUtil.class);

  public static final String MODE_EMBEDDED = "embedded";

  public static void checkFlinkVersion() {
    String flinkVersion = EnvironmentInformation.getVersion();
    if (!flinkVersion.startsWith("1.14")) {
      LOG.error("Only Flink-1.14 is supported now!");
      throw new RuntimeException("Only Flink-1.14 is supported now!");
    }
  }

  public static CliOptions parseCliOptions(String[] args) {
    final String mode;
    final String[] modeArgs;
    if (args.length < 1 || args[0].startsWith("-")) {
      // mode is not specified, use the default `embedded` mode
      mode = MODE_EMBEDDED;
      modeArgs = args;
    } else {
      // mode is specified, extract the mode value and reaming args
      mode = args[0];
      // remove mode
      modeArgs = Arrays.copyOfRange(args, 1, args.length);
    }

    final CliOptions options = CliOptionsParser.parseEmbeddedModeClient(modeArgs);

    switch (mode) {
      case MODE_EMBEDDED:
        if (options.isPrintHelp()) {
          CliOptionsParser.printHelpEmbeddedModeClient();
        }
        break;

      default:
        throw new SqlClientException("Other mode is not supported yet.");
    }

    return options;
  }
}
