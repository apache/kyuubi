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
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.core.fs.Path;

/** Parser for engine options. */
public class EngineOptionsParser {

  public static final Option OPTION_HELP =
      Option.builder("h")
          .required(false)
          .longOpt("help")
          .desc("Show the help message with descriptions of all options.")
          .build();

  public static final Option OPTION_PORT =
      Option.builder("p")
          .required(false)
          .longOpt("port")
          .numberOfArgs(1)
          .argName("service port")
          .desc("The port to which the REST client connects to.")
          .build();

  public static final Option OPTION_DEFAULTS =
      Option.builder("d")
          .required(false)
          .longOpt("defaults")
          .numberOfArgs(1)
          .argName("default configuration file")
          .desc(
              "The properties with which every new session is initialized. "
                  + "Properties might be overwritten by session properties.")
          .build();

  public static final Option OPTION_JAR =
      Option.builder("j")
          .required(false)
          .longOpt("jar")
          .numberOfArgs(1)
          .argName("JAR file")
          .desc(
              "A JAR file to be imported into the session. The file might contain "
                  + "user-defined classes needed for the execution of statements such as "
                  + "functions, table sources, or sinks. Can be used multiple times.")
          .build();

  public static final Option OPTION_LIBRARY =
      Option.builder("l")
          .required(false)
          .longOpt("library")
          .numberOfArgs(1)
          .argName("JAR directory")
          .desc(
              "A JAR file directory with which every new session is initialized. The files might "
                  + "contain user-defined classes needed for the execution of statements such as "
                  + "functions, table sources, or sinks. Can be used multiple times.")
          .build();

  private static final Options ENGINE_OPTIONS = getEngineOptions();

  // --------------------------------------------------------------------------------------------
  //  Help
  // --------------------------------------------------------------------------------------------

  /** Prints the help. */
  public static void printHelp() {
    System.out.println("./flink-sql-engine [OPTIONS]");
    System.out.println();
    System.out.println("The following options are available:");

    HelpFormatter formatter = new HelpFormatter();
    formatter.setLeftPadding(5);
    formatter.setWidth(80);

    formatter.printHelp(" ", ENGINE_OPTIONS);

    System.out.println();
  }

  // --------------------------------------------------------------------------------------------
  //  Line Parsing
  // --------------------------------------------------------------------------------------------

  public static EngineOptions parseEngineOptions(String[] args) {
    try {
      DefaultParser parser = new DefaultParser();
      CommandLine line = parser.parse(ENGINE_OPTIONS, args, true);
      Integer port = null;
      if (line.hasOption(EngineOptionsParser.OPTION_PORT.getOpt())) {
        port = Integer.valueOf(line.getOptionValue(EngineOptionsParser.OPTION_PORT.getOpt()));
      }
      return new EngineOptions(
          line.hasOption(EngineOptionsParser.OPTION_HELP.getOpt()),
          port,
          checkUrl(line, EngineOptionsParser.OPTION_DEFAULTS),
          checkUrls(line, EngineOptionsParser.OPTION_JAR),
          checkUrls(line, EngineOptionsParser.OPTION_LIBRARY));
    } catch (ParseException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  // --------------------------------------------------------------------------------------------

  private static Options getEngineOptions() {
    Options options = new Options();
    options.addOption(OPTION_HELP);
    options.addOption(OPTION_DEFAULTS);
    options.addOption(OPTION_PORT);
    options.addOption(OPTION_JAR);
    options.addOption(OPTION_LIBRARY);
    return options;
  }

  private static URL checkUrl(CommandLine line, Option option) {
    final List<URL> urls = checkUrls(line, option);
    if (urls != null && !urls.isEmpty()) {
      return urls.get(0);
    }
    return null;
  }

  private static List<URL> checkUrls(CommandLine line, Option option) {
    if (line.hasOption(option.getOpt())) {
      final String[] urls = line.getOptionValues(option.getOpt());
      return Arrays.stream(urls)
          .distinct()
          .map(
              (url) -> {
                try {
                  return Path.fromLocalFile(new File(url).getAbsoluteFile()).toUri().toURL();
                } catch (Exception e) {
                  throw new IllegalArgumentException(
                      "Invalid path for option '" + option.getLongOpt() + "': " + url, e);
                }
              })
          .collect(Collectors.toList());
    }
    return null;
  }
}
