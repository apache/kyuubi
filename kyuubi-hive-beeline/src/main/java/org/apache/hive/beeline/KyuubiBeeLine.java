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

import java.io.IOException;
import java.io.InputStream;
import java.sql.Driver;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kyuubi.util.reflect.DynConstructors;
import org.apache.kyuubi.util.reflect.DynFields;
import org.apache.kyuubi.util.reflect.DynMethods;

public class KyuubiBeeLine extends BeeLine {
  public static final String KYUUBI_BEELINE_DEFAULT_JDBC_DRIVER =
      "org.apache.kyuubi.jdbc.KyuubiHiveDriver";
  protected KyuubiCommands commands = new KyuubiCommands(this);
  private Driver defaultDriver;

  public KyuubiBeeLine() {
    this(true);
  }

  @SuppressWarnings("deprecation")
  public KyuubiBeeLine(boolean isBeeLine) {
    super(isBeeLine);
    try {
      DynFields.builder().hiddenImpl(BeeLine.class, "commands").buildChecked(this).set(commands);
    } catch (Throwable t) {
      throw new ExceptionInInitializerError("Failed to inject kyuubi commands");
    }
    try {
      defaultDriver =
          DynConstructors.builder()
              .impl(KYUUBI_BEELINE_DEFAULT_JDBC_DRIVER)
              .<Driver>buildChecked()
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
          "Apache Kyuubi",
        });
  }

  @Override
  int initArgs(String[] args) {
    List<String> commands = Collections.emptyList();

    CommandLine cl;
    BeelineParser beelineParser;
    boolean connSuccessful;
    boolean exit;
    DynFields.BoundField<Boolean> exitField;

    try {
      Options options =
          DynFields.builder()
              .hiddenImpl(BeeLine.class, "options")
              .<Options>buildStaticChecked()
              .get();

      beelineParser = new BeelineParser();
      cl = beelineParser.parse(options, args);

      connSuccessful =
          DynMethods.builder("connectUsingArgs")
              .hiddenImpl(BeeLine.class, BeelineParser.class, CommandLine.class)
              .buildChecked(this)
              .invoke(beelineParser, cl);

      exitField = DynFields.builder().hiddenImpl(BeeLine.class, "exit").buildChecked(this);
      exit = exitField.get();

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
        connSuccessful =
            DynMethods.builder("defaultBeelineConnect")
                .hiddenImpl(BeeLine.class, CommandLine.class)
                .buildChecked(this)
                .invoke(beelineParser, cl);

      } catch (Exception t) {
        error(t.getMessage());
        return 1;
      }
    }

    int code = 0;
    if (cl.getOptionValues('e') != null) {
      commands = Arrays.asList(cl.getOptionValues('e'));
      // When using -e, command is always a single line, see HIVE-19018
      getOpts().setAllowMultiLineCommand(false);
    }

    if (!commands.isEmpty() && getOpts().getScriptFile() != null) {
      error("The '-e' and '-f' options cannot be specified simultaneously");
      return 1;
    } else if (!commands.isEmpty() && !connSuccessful) {
      error("Cannot run commands specified using -e. No current connection");
      return 1;
    }
    if (!commands.isEmpty()) {
      for (String command : commands) {
        debug(loc("executing-command", command));
        if (!dispatch(command)) {
          code++;
        }
      }
      try {
        exit = true;
        exitField.set(exit);
      } catch (Exception e) {
        error(e.getMessage());
        return 1;
      }
    }
    return code;
  }
}
