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

package org.apache.hive.beeline;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.sql.Driver;
import java.text.MessageFormat;
import java.util.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class KyuubiBeeLine extends BeeLine {
  private static final ResourceBundle resourceBundle =
      ResourceBundle.getBundle(KyuubiBeeLine.class.getSimpleName());
  public static final String KYUUBI_BEELINE_DEFAULT_JDBC_DRIVER =
      "org.apache.kyuubi.jdbc.KyuubiHiveDriver";
  protected KyuubiCommands commands = new KyuubiCommands(this);
  private Driver defaultDriver = null;

  protected String kyuubiBatchRequest;

  public KyuubiBeeLine() {
    this(true);
  }

  public KyuubiBeeLine(boolean isBeeLine) {
    super(isBeeLine);
    try {
      Field commandsField = BeeLine.class.getDeclaredField("commands");
      commandsField.setAccessible(true);
      commandsField.set(this, commands);
    } catch (Throwable t) {
      throw new ExceptionInInitializerError("Failed to inject kyuubi commands");
    }
    try {
      defaultDriver =
          (Driver)
              Class.forName(
                      KYUUBI_BEELINE_DEFAULT_JDBC_DRIVER,
                      true,
                      Thread.currentThread().getContextClassLoader())
                  .newInstance();
    } catch (Throwable t) {
      throw new ExceptionInInitializerError(KYUUBI_BEELINE_DEFAULT_JDBC_DRIVER + "-missing");
    }
  }

  /** Starts the program. */
  public static void main(String[] args) throws IOException {
    mainWithInputRedirection(args, null);
  }

  /**
   * Starts the program with redirected input. For redirected output, setOutputStream() and
   * setErrorStream can be used. Exits with 0 on success, 1 on invalid arguments, and 2 on any other
   * error
   *
   * @param args same as main()
   * @param inputStream redirected input, or null to use standard input
   */
  public static void mainWithInputRedirection(String[] args, InputStream inputStream)
      throws IOException {
    KyuubiBeeLine beeLine = new KyuubiBeeLine();
    try {
      int status = beeLine.begin(args, inputStream);

      if (!Boolean.getBoolean(BeeLineOpts.PROPERTY_NAME_EXIT)) {
        System.exit(status);
      }
    } finally {
      beeLine.close();
    }
  }

  protected Driver getDefaultDriver() {
    return defaultDriver;
  }

  @Override
  String getApplicationTitle() {
    Package pack = BeeLine.class.getPackage();

    return loc(
        "app-introduction",
        new Object[] {
          "Beeline",
          pack.getImplementationVersion() == null ? "???" : pack.getImplementationVersion(),
          "Apache Kyuubi (Incubating)",
        });
  }

  @Override
  int initArgs(String[] args) {
    List<String> commands = Collections.emptyList();

    CommandLine cl;
    BeelineParser beelineParser;
    boolean connSuccessful;
    boolean exit;
    Field exitField;

    try {
      Field optionsField = BeeLine.class.getDeclaredField("options");
      optionsField.setAccessible(true);
      Options options = (Options) optionsField.get(this);

      beelineParser = new BeelineParser();
      cl = beelineParser.parse(options, args);

      if (cl.getOptionValues('e') != null) {
        commands = Arrays.asList(cl.getOptionValues('e'));
        // When using -e, command is always a single line, see HIVE-19018
        getOpts().setAllowMultiLineCommand(false);
      }

      if (cl.getOptionValue("kyuubi-batch") != null) {
        if (!commands.isEmpty()) {
          if (commands.size() > 1) {
            info("The kyuubi batch request only takes the first query specified with '-e'.");
          }
          kyuubiBatchRequest = commands.get(0);
        }

        if (kyuubiBatchRequest == null && getOpts().getScriptFile() != null) {
          File scriptFile = new File(getOpts().getScriptFile());
          try {
            kyuubiBatchRequest =
                String.join("\n", Files.readLines(scriptFile, Charset.forName("UTF-8")));
          } catch (IOException e) {
            error(e.getMessage());
            return 1;
          }
        }

        if (kyuubiBatchRequest == null) {
          error("In kyuubi batch mode, please specify the batch request with '-e' or '-f' option.");
          return 1;
        }
      }

      Method connectUsingArgsMethod =
          BeeLine.class.getDeclaredMethod(
              "connectUsingArgs", BeelineParser.class, CommandLine.class);
      connectUsingArgsMethod.setAccessible(true);
      connSuccessful = (boolean) connectUsingArgsMethod.invoke(this, beelineParser, cl);

      exitField = BeeLine.class.getDeclaredField("exit");
      exitField.setAccessible(true);
      exit = (boolean) exitField.get(this);

    } catch (ParseException e1) {
      output(e1.getMessage());
      usage();
      return -1;
    } catch (Exception t) {
      error(t.getMessage());
      return 1;
    }

    // checks if default hs2 connection configuration file is present
    // and uses it to connect if found
    // no-op if the file is not present
    if (!connSuccessful && !exit) {
      try {
        Method defaultBeelineConnectMethod =
            BeeLine.class.getDeclaredMethod("defaultBeelineConnect");
        defaultBeelineConnectMethod.setAccessible(true);
        connSuccessful = (boolean) defaultBeelineConnectMethod.invoke(this);

      } catch (Exception t) {
        error(t.getMessage());
        return 1;
      }
    }

    int code = 0;
    if (!commands.isEmpty() && getOpts().getScriptFile() != null) {
      error("The '-e' and '-f' options cannot be specified simultaneously");
      return 1;
    } else if (!commands.isEmpty() && !connSuccessful) {
      error("Cannot run commands specified using -e. No current connection");
      return 1;
    }
    if (!commands.isEmpty()) {
      for (Iterator<String> i = commands.iterator(); i.hasNext(); ) {
        String command = i.next().toString();
        debug(loc("executing-command", command));
        if (!dispatch(command)) {
          code++;
        }
      }
      try {
        exit = true;
        exitField.set(this, exit);
      } catch (Exception e) {
        error(e.getMessage());
        return 1;
      }
    }
    return code;
  }

  static {
    try {
      Field optionsFields = BeeLine.class.getDeclaredField("options");
      optionsFields.setAccessible(true);
      Options options = (Options) optionsFields.get(null);
      options.addOption(
          OptionBuilder.withLongOpt("kyuubi-batch")
              .withDescription("whether to enable kyuubi batch mode to submit batch job")
              .create());
    } catch (Throwable t) {
      throw new ExceptionInInitializerError("Failed to inject kyuubi options.");
    }
  }

  @Override
  void usage() {
    super.usage();
    String kyuubiExtUsage =
        MessageFormat.format(resourceBundle.getString("cmd-usage"), new Object[0]);
    output(kyuubiExtUsage);
  }
}
