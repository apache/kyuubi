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

import static org.apache.kyuubi.jdbc.hive.JdbcConnectionParams.*;

import java.io.*;
import java.sql.*;
import java.util.*;
import org.apache.hive.beeline.logs.KyuubiBeelineInPlaceUpdateStream;
import org.apache.kyuubi.jdbc.hive.KyuubiStatement;
import org.apache.kyuubi.jdbc.hive.Utils;
import org.apache.kyuubi.jdbc.hive.logs.InPlaceUpdateStream;
import org.apache.kyuubi.jdbc.hive.logs.KyuubiLoggable;

public class KyuubiCommands extends Commands {
  protected KyuubiBeeLine beeLine;
  protected static final int DEFAULT_QUERY_PROGRESS_INTERVAL = 1000;
  protected static final int DEFAULT_QUERY_PROGRESS_THREAD_TIMEOUT = 10 * 1000;

  public KyuubiCommands(KyuubiBeeLine beeLine) {
    super(beeLine);
    this.beeLine = beeLine;
  }

  @Override
  public boolean sql(String line) {
    return execute(line, false, false);
  }

  /** Extract and clean up the first command in the input. */
  private String getFirstCmd(String cmd, int length) {
    return cmd.substring(length);
  }

  private String[] tokenizeCmd(String cmd) {
    return cmd.split("\\s+");
  }

  private boolean isSourceCMD(String cmd) {
    if (cmd == null || cmd.isEmpty()) return false;
    String[] tokens = tokenizeCmd(cmd);
    return tokens[0].equalsIgnoreCase("source");
  }

  private boolean sourceFile(String cmd) {
    String[] tokens = tokenizeCmd(cmd);
    String cmd_1 = getFirstCmd(cmd, tokens[0].length());

    cmd_1 = substituteVariables(getHiveConf(false), cmd_1);
    File sourceFile = new File(cmd_1);
    if (!sourceFile.isFile()) {
      return false;
    } else {
      boolean ret;
      try {
        ret = sourceFileInternal(sourceFile);
      } catch (IOException e) {
        beeLine.error(e);
        return false;
      }
      return ret;
    }
  }

  private boolean sourceFileInternal(File sourceFile) throws IOException {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(sourceFile));
      String extra = reader.readLine();
      String lines = null;
      while (extra != null) {
        if (beeLine.isComment(extra)) {
          continue;
        }
        if (lines == null) {
          lines = extra;
        } else {
          lines += "\n" + extra;
        }
        extra = reader.readLine();
      }
      String[] cmds = lines.split(";");
      for (String c : cmds) {
        if (!executeInternal(c, false)) {
          return false;
        }
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
    return true;
  }

  // Return false only occurred error when execution the sql and the sql should follow the rules
  // of beeline.
  private boolean executeInternal(String sql, boolean call) {
    if (!beeLine.isBeeLine()) {
      sql = cliToBeelineCmd(sql);
    }

    if (sql == null || sql.length() == 0) {
      return true;
    }

    if (beeLine.isComment(sql)) {
      // skip this and rest cmds in the line
      return true;
    }

    // is source CMD
    if (isSourceCMD(sql)) {
      return sourceFile(sql);
    }

    if (sql.startsWith(BeeLine.COMMAND_PREFIX)) {
      return beeLine.execCommandWithPrefix(sql);
    }

    String prefix = call ? "call" : "sql";

    if (sql.startsWith(prefix)) {
      sql = sql.substring(prefix.length());
    }

    // batch statements?
    if (beeLine.getBatch() != null) {
      beeLine.getBatch().add(sql);
      return true;
    }

    if (!(beeLine.assertConnection())) {
      return false;
    }

    ClientHook hook = ClientCommandHookFactory.get().getHook(beeLine, sql);

    try {
      Statement stmnt = null;
      boolean hasResults;
      Thread logThread = null;

      try {
        long start = System.currentTimeMillis();

        if (call) {
          stmnt = beeLine.getDatabaseConnection().getConnection().prepareCall(sql);
          hasResults = ((CallableStatement) stmnt).execute();
        } else {
          stmnt = beeLine.createStatement();
          if (beeLine.getOpts().isSilent()) {
            hasResults = stmnt.execute(sql);
          } else {
            InPlaceUpdateStream.EventNotifier eventNotifier =
                new InPlaceUpdateStream.EventNotifier();
            logThread = new Thread(createLogRunnable(stmnt, eventNotifier));
            logThread.setDaemon(true);
            logThread.start();
            if (stmnt instanceof KyuubiStatement) {
              KyuubiStatement kyuubiStatement = (KyuubiStatement) stmnt;
              kyuubiStatement.setInPlaceUpdateStream(
                  new KyuubiBeelineInPlaceUpdateStream(beeLine.getErrorStream(), eventNotifier));
            }
            hasResults = stmnt.execute(sql);
            logThread.interrupt();
            logThread.join(DEFAULT_QUERY_PROGRESS_THREAD_TIMEOUT);
          }
        }

        beeLine.showWarnings();

        if (hasResults) {
          do {
            ResultSet rs = stmnt.getResultSet();
            try {
              int count = beeLine.print(rs);
              long end = System.currentTimeMillis();

              beeLine.info(
                  beeLine.loc("rows-selected", count) + " " + beeLine.locElapsedTime(end - start));
            } finally {
              if (logThread != null) {
                logThread.join(DEFAULT_QUERY_PROGRESS_THREAD_TIMEOUT);
                showRemainingLogsIfAny(stmnt);
                logThread = null;
              }
              rs.close();
            }
          } while (BeeLine.getMoreResults(stmnt));
        } else {
          int count = stmnt.getUpdateCount();
          long end = System.currentTimeMillis();
          beeLine.info(
              beeLine.loc("rows-affected", count) + " " + beeLine.locElapsedTime(end - start));
        }
      } finally {
        if (logThread != null) {
          if (!logThread.isInterrupted()) {
            logThread.interrupt();
          }
          logThread.join(DEFAULT_QUERY_PROGRESS_THREAD_TIMEOUT);
          showRemainingLogsIfAny(stmnt);
        }
        if (stmnt != null) {
          stmnt.close();
        }
      }
    } catch (Exception e) {
      return beeLine.error(e);
    }
    beeLine.showWarnings();
    if (hook != null) {
      hook.postHook(beeLine);
    }
    return true;
  }

  @Override
  public boolean sql(String line, boolean entireLineAsCommand) {
    return execute(line, false, entireLineAsCommand);
  }

  @Override
  public boolean call(String line) {
    return execute(line, true, false);
  }

  private boolean execute(String line, boolean call, boolean entireLineAsCommand) {
    if (line == null || line.length() == 0) {
      return false; // ???
    }

    // ### FIXME: doing the multi-line handling down here means
    // higher-level logic never sees the extra lines. So,
    // for example, if a script is being saved, it won't include
    // the continuation lines! This is logged as sf.net
    // bug 879518.

    // use multiple lines for statements not terminated by ";"
    try {
      line = handleMultiLineCmd(line);
    } catch (Exception e) {
      beeLine.handleException(e);
    }

    List<String> cmdList = getCmdList(line, entireLineAsCommand);
    for (int i = 0; i < cmdList.size(); i++) {
      String sql = cmdList.get(i);
      if (sql.length() != 0) {
        if (!executeInternal(sql, call)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Helper method to parse input from Beeline and convert it to a {@link List} of commands that can
   * be executed. This method contains logic for handling semicolons that are placed within
   * quotations. It iterates through each character in the line and checks to see if it is a ;, ',
   * or "
   */
  private List<String> getCmdList(String line, boolean entireLineAsCommand) {
    List<String> cmdList = new ArrayList<String>();
    if (entireLineAsCommand) {
      cmdList.add(line);
    } else {
      StringBuilder command = new StringBuilder();

      // Marker to track if there is starting double quote without an ending double quote
      boolean hasUnterminatedDoubleQuote = false;

      // Marker to track if there is starting single quote without an ending double quote
      boolean hasUnterminatedSingleQuote = false;

      // Index of the last seen semicolon in the given line
      int lastSemiColonIndex = 0;
      char[] lineChars = line.toCharArray();

      // Marker to track if the previous character was an escape character
      boolean wasPrevEscape = false;

      int index = 0;

      // Iterate through the line and invoke the addCmdPart method whenever a semicolon is seen that
      // is not inside a
      // quoted string
      for (; index < lineChars.length; index++) {
        switch (lineChars[index]) {
          case '\'':
            // If a single quote is seen and the index is not inside a double quoted string and the
            // previous character
            // was not an escape, then update the hasUnterminatedSingleQuote flag
            if (!hasUnterminatedDoubleQuote && !wasPrevEscape) {
              hasUnterminatedSingleQuote = !hasUnterminatedSingleQuote;
            }
            wasPrevEscape = false;
            break;
          case '\"':
            // If a double quote is seen and the index is not inside a single quoted string and the
            // previous character
            // was not an escape, then update the hasUnterminatedDoubleQuote flag
            if (!hasUnterminatedSingleQuote && !wasPrevEscape) {
              hasUnterminatedDoubleQuote = !hasUnterminatedDoubleQuote;
            }
            wasPrevEscape = false;
            break;
          case ';':
            // If a semicolon is seen, and the line isn't inside a quoted string, then treat
            // line[lastSemiColonIndex] to line[index] as a single command
            if (!hasUnterminatedDoubleQuote && !hasUnterminatedSingleQuote) {
              addCmdPart(cmdList, command, line.substring(lastSemiColonIndex, index));
              lastSemiColonIndex = index + 1;
            }
            wasPrevEscape = false;
            break;
          case '\\':
            wasPrevEscape = !wasPrevEscape;
            break;
          default:
            wasPrevEscape = false;
            break;
        }
      }
      // If the line doesn't end with a ; or if the line is empty, add the cmd part
      if (lastSemiColonIndex != index || lineChars.length == 0) {
        addCmdPart(cmdList, command, line.substring(lastSemiColonIndex, index));
      }
    }
    return cmdList;
  }

  /**
   * Given a cmdpart (e.g. if a command spans multiple lines), add to the current command, and if
   * applicable add that command to the {@link List} of commands
   */
  private void addCmdPart(List<String> cmdList, StringBuilder command, String cmdpart) {
    if (cmdpart.endsWith("\\")) {
      command.append(cmdpart.substring(0, cmdpart.length() - 1)).append(";");
      return;
    } else {
      command.append(cmdpart);
    }
    cmdList.add(command.toString());
    command.setLength(0);
  }

  protected Runnable createLogRunnable(
      final Object sqlObject, InPlaceUpdateStream.EventNotifier eventNotifier) {
    if (sqlObject instanceof KyuubiLoggable) {
      return new KyuubiLogRunnable(
          this, (KyuubiLoggable) sqlObject, DEFAULT_QUERY_PROGRESS_INTERVAL, eventNotifier);
    } else {
      beeLine.debug("The instance is not KyuubiLoggable type: " + sqlObject.getClass());
      return new Runnable() {
        @Override
        public void run() {
          // do nothing.
        }
      };
    }
  }

  private void showRemainingLogsIfAny(Object sqlObject) {
    if (sqlObject instanceof KyuubiLoggable) {
      KyuubiLoggable kyuubiLoggable = (KyuubiLoggable) sqlObject;
      List<String> logs = null;
      do {
        try {
          logs = kyuubiLoggable.getExecLog();
        } catch (SQLException e) {
          beeLine.error(new SQLWarning(e));
          return;
        }
        for (String log : logs) {
          beeLine.info(log);
        }
      } while (logs.size() > 0);
    } else {
      beeLine.debug("The instance is not KyuubiLoggable type: " + sqlObject.getClass());
    }
  }

  private String getProperty(Properties props, String[] keys) {
    for (int i = 0; i < keys.length; i++) {
      String val = props.getProperty(keys[i]);
      if (val != null) {
        return val;
      }
    }

    for (Iterator i = props.keySet().iterator(); i.hasNext(); ) {
      String key = (String) i.next();
      for (int j = 0; j < keys.length; j++) {
        if (key.endsWith(keys[j])) {
          return props.getProperty(key);
        }
      }
    }

    return null;
  }

  public boolean connect(Properties props) throws IOException {
    String url =
        getProperty(
            props,
            new String[] {
              PROPERTY_URL, "javax.jdo.option.ConnectionURL", "ConnectionURL",
            });
    String driver =
        getProperty(
            props,
            new String[] {
              PROPERTY_DRIVER, "javax.jdo.option.ConnectionDriverName", "ConnectionDriverName",
            });
    String username =
        getProperty(
            props,
            new String[] {
              AUTH_USER, "javax.jdo.option.ConnectionUserName", "ConnectionUserName",
            });
    String password =
        getProperty(
            props,
            new String[] {
              AUTH_PASSWD, "javax.jdo.option.ConnectionPassword", "ConnectionPassword",
            });

    if (url == null || url.length() == 0) {
      return beeLine.error("Property \"url\" is required");
    }
    if (driver == null || driver.length() == 0) {
      if (!beeLine.scanForDriver(url)) {
        return beeLine.error(beeLine.loc("no-driver", url));
      }
    }

    String auth = getProperty(props, new String[] {AUTH_TYPE});
    if (auth == null) {
      auth = beeLine.getOpts().getAuthType();
      if (auth != null) {
        props.setProperty(AUTH_TYPE, auth);
      }
    }

    beeLine.info("Connecting to " + url);
    if (Utils.parsePropertyFromUrl(url, AUTH_PRINCIPAL) == null
        || Utils.parsePropertyFromUrl(url, AUTH_KYUUBI_SERVER_PRINCIPAL) == null) {
      String urlForPrompt = url.substring(0, url.contains(";") ? url.indexOf(';') : url.length());
      if (username == null) {
        username = beeLine.getConsoleReader().readLine("Enter username for " + urlForPrompt + ": ");
      }
      props.setProperty(AUTH_USER, username);
      if (password == null) {
        password =
            beeLine
                .getConsoleReader()
                .readLine("Enter password for " + urlForPrompt + ": ", new Character('*'));
      }
      props.setProperty(AUTH_PASSWD, password);
    }

    try {
      beeLine
          .getDatabaseConnections()
          .setConnection(new KyuubiDatabaseConnection(beeLine, driver, url, props));
      beeLine.getDatabaseConnection().getConnection();

      if (!beeLine.isBeeLine()) {
        beeLine.updateOptsForCli();
      }
      beeLine.runInit();

      beeLine.setCompletions();
      beeLine.getOpts().setLastConnectedUrl(url);
      return true;
    } catch (SQLException sqle) {
      beeLine.getDatabaseConnections().remove();
      return beeLine.error(sqle);
    } catch (IOException ioe) {
      return beeLine.error(ioe);
    }
  }

  @Override
  public String handleMultiLineCmd(String line) throws IOException {
    int[] startQuote = {-1};
    Character mask =
        (System.getProperty("jline.terminal", "").equals("jline.UnsupportedTerminal"))
            ? null
            : jline.console.ConsoleReader.NULL_MASK;

    while (isMultiLine(line) && beeLine.getOpts().isAllowMultiLineCommand()) {
      StringBuilder prompt = new StringBuilder(beeLine.getPrompt());
      if (!beeLine.getOpts().isSilent()) {
        for (int i = 0; i < prompt.length() - 1; i++) {
          if (prompt.charAt(i) != '>') {
            prompt.setCharAt(i, i % 2 == 0 ? '.' : ' ');
          }
        }
      }
      String extra;
      // avoid NPE below if for some reason -e argument has multi-line command
      if (beeLine.getConsoleReader() == null) {
        throw new RuntimeException(
            "Console reader not initialized. This could happen when there "
                + "is a multi-line command using -e option and which requires further reading from console");
      }
      if (beeLine.getOpts().isSilent() && beeLine.getOpts().getScriptFile() != null) {
        extra = beeLine.getConsoleReader().readLine(null, mask);
      } else {
        extra = beeLine.getConsoleReader().readLine(prompt.toString());
      }

      if (extra == null) { // it happens when using -f and the line of cmds does not end with ;
        break;
      }
      if (!extra.isEmpty()) {
        line += "\n" + extra;
      }
    }
    return line;
  }

  // returns true if statement represented by line is not complete and needs additional reading from
  // console. Used in handleMultiLineCmd method assumes line would never be null when this method is
  // called
  private boolean isMultiLine(String line) {
    if (line.endsWith(beeLine.getOpts().getDelimiter()) || beeLine.isComment(line)) {
      return false;
    }
    // handles the case like line = show tables; --test comment
    List<String> cmds = getCmdList(line, false);
    return cmds.isEmpty() || !cmds.get(cmds.size() - 1).startsWith("--");
  }

  static class KyuubiLogRunnable implements Runnable {
    private final KyuubiCommands commands;
    private final KyuubiLoggable kyuubiLoggable;
    private final long queryProgressInterval;
    private final InPlaceUpdateStream.EventNotifier notifier;

    KyuubiLogRunnable(
        KyuubiCommands commands,
        KyuubiLoggable kyuubiLoggable,
        long queryProgressInterval,
        InPlaceUpdateStream.EventNotifier eventNotifier) {
      this.kyuubiLoggable = kyuubiLoggable;
      this.commands = commands;
      this.queryProgressInterval = queryProgressInterval;
      this.notifier = eventNotifier;
    }

    private void updateExecLog() {
      try {
        List<String> execLogs = kyuubiLoggable.getExecLog();
        for (String log : execLogs) {
          commands.beeLine.info(log);
        }
        if (!execLogs.isEmpty()) {
          notifier.operationLogShowedToUser();
        }
      } catch (SQLException e) {
        commands.beeLine.error(new SQLWarning(e));
      }
    }

    @Override
    public void run() {
      try {
        while (kyuubiLoggable.hasMoreLogs()) {
          /*
            get the operation logs once and print it, then wait till progress bar update is complete
            before printing the remaining logs.
          */
          if (notifier.canOutputOperationLogs()) {
            commands.beeLine.debug("going to print operations logs");
            updateExecLog();
            commands.beeLine.debug("printed operations logs");
          }
          Thread.sleep(queryProgressInterval);
        }
      } catch (InterruptedException e) {
        commands.beeLine.debug("Getting log thread is interrupted, since query is done!");
      } finally {
        commands.showRemainingLogsIfAny(kyuubiLoggable);
      }
    }
  }
}
