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

import org.apache.hadoop.io.IOUtils;
import org.apache.hive.beeline.logs.KyuubiBeelineInPlaceUpdateStream;
import org.apache.kyuubi.jdbc.hive.KyuubiConnection;

import org.apache.kyuubi.jdbc.hive.KyuubiStatement;
import org.apache.kyuubi.jdbc.hive.logs.InPlaceUpdateStream;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.*;

import org.apache.kyuubi.jdbc.hive.Utils;
import org.apache.kyuubi.jdbc.hive.Utils.JdbcConnectionParams;

public class KyuubiCommands extends Commands {
  protected KyuubiBeeLine beeLine;
  private static final int DEFAULT_QUERY_PROGRESS_INTERVAL = 1000;
  private static final int DEFAULT_QUERY_PROGRESS_THREAD_TIMEOUT = 10 * 1000;

  public KyuubiCommands(KyuubiBeeLine beeLine) {
    super(beeLine);
    this.beeLine = beeLine;
  }

  public boolean sql(String line) {
    return execute(line, false, false);
  }

  /**
   * Extract and clean up the first command in the input.
   */
  private String getFirstCmd(String cmd, int length) {
    return cmd.substring(length).trim();
  }

  private String[] tokenizeCmd(String cmd) {
    return cmd.split("\\s+");
  }

  private boolean isSourceCMD(String cmd) {
    if (cmd == null || cmd.isEmpty())
      return false;
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
        c = c.trim();
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
      //skip this and rest cmds in the line
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
                new KyuubiBeelineInPlaceUpdateStream(
                  beeLine.getErrorStream(),
                  eventNotifier
                ));
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

  public boolean sql(String line, boolean entireLineAsCommand) {
    return execute(line, false, entireLineAsCommand);
  }

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

    line = line.trim();
    List<String> cmdList = getCmdList(line, entireLineAsCommand);
    for (int i = 0; i < cmdList.size(); i++) {
      String sql = cmdList.get(i).trim();
      if (sql.length() != 0) {
        if (!executeInternal(sql, call)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Helper method to parse input from Beeline and convert it to a {@link List} of commands that
   * can be executed. This method contains logic for handling semicolons that are placed within
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

      // Iterate through the line and invoke the addCmdPart method whenever a semicolon is seen that is not inside a
      // quoted string
      for (; index < lineChars.length; index++) {
        switch (lineChars[index]) {
          case '\'':
            // If a single quote is seen and the index is not inside a double quoted string and the previous character
            // was not an escape, then update the hasUnterminatedSingleQuote flag
            if (!hasUnterminatedDoubleQuote && !wasPrevEscape) {
              hasUnterminatedSingleQuote = !hasUnterminatedSingleQuote;
            }
            wasPrevEscape = false;
            break;
          case '\"':
            // If a double quote is seen and the index is not inside a single quoted string and the previous character
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

  private Runnable createLogRunnable(final Statement statement,
                                     InPlaceUpdateStream.EventNotifier eventNotifier) {
    if (statement instanceof KyuubiStatement) {
      return new KyuubiStatementLogRunnable(this, (KyuubiStatement) statement, DEFAULT_QUERY_PROGRESS_INTERVAL,
        eventNotifier);
    } else {
      beeLine.debug(
        "The statement instance is not HiveStatement type: " + statement
          .getClass());
      return new Runnable() {
        @Override
        public void run() {
          // do nothing.
        }
      };
    }
  }

  private void error(Throwable throwable) {
    beeLine.error(throwable);
  }

  private void debug(String message) {
    beeLine.debug(message);
  }

  private void showRemainingLogsIfAny(Statement statement) {
    if (statement instanceof KyuubiStatement) {
      KyuubiStatement kyuubiStatement = (KyuubiStatement) statement;
      List<String> logs = null;
      do {
        try {
          logs = kyuubiStatement.getQueryLog();
        } catch (SQLException e) {
          beeLine.error(new SQLWarning(e));
          return;
        }
        for (String log : logs) {
          beeLine.info(log);
        }
      } while (logs.size() > 0);
    } else {
      beeLine.debug("The statement instance is not HiveStatement type: " + statement.getClass());
    }
  }

  public boolean quit(String line) {
    beeLine.setExit(true);
    close(null);
    return true;
  }

  public boolean exit(String line) {
    return quit(line);
  }

  /**
   * Close all connections.
   */
  public boolean closeall(String line) {
    if (close(null)) {
      while (close(null)) {
        ;
      }
      return true;
    }
    return false;
  }


  /**
   * Close the current connection.
   */
  public boolean close(String line) {
    if (beeLine.getDatabaseConnection() == null) {
      return false;
    }
    try {
      if (beeLine.getDatabaseConnection().getCurrentConnection() != null
        && !(beeLine.getDatabaseConnection().getCurrentConnection().isClosed())) {
        int index = beeLine.getDatabaseConnections().getIndex();
        beeLine.info(beeLine.loc("closing", index, beeLine.getDatabaseConnection()));
        beeLine.getDatabaseConnection().getCurrentConnection().close();
      } else {
        beeLine.info(beeLine.loc("already-closed"));
      }
    } catch (Exception e) {
      return beeLine.error(e);
    }
    beeLine.getDatabaseConnections().remove();
    return true;
  }


  /**
   * Connect to the database defined in the specified properties file.
   */
  public boolean properties(String line) throws Exception {
    String example = "";
    example += "Usage: properties <properties file>" + BeeLine.getSeparator();

    String[] parts = beeLine.split(line);
    if (parts.length < 2) {
      return beeLine.error(example);
    }

    int successes = 0;

    for (int i = 1; i < parts.length; i++) {
      Properties props = new Properties();
      InputStream stream = new FileInputStream(parts[i]);
      try {
        props.load(stream);
      } finally {
        IOUtils.closeStream(stream);
      }
      if (connect(props)) {
        successes++;
      }
    }

    if (successes != (parts.length - 1)) {
      return false;
    } else {
      return true;
    }
  }


  public boolean connect(String line) throws Exception {
    String example = "Usage: connect <url> <username> <password> [driver]"
      + BeeLine.getSeparator();

    String[] parts = beeLine.split(line);
    if (parts == null) {
      return false;
    }

    if (parts.length < 2) {
      return beeLine.error(example);
    }

    String url = parts.length < 2 ? null : parts[1];
    String user = parts.length < 3 ? null : parts[2];
    String pass = parts.length < 4 ? null : parts[3];
    String driver = parts.length < 5 ? null : parts[4];

    Properties props = new Properties();
    if (url != null) {
      String saveUrl = getUrlToUse(url);
      props.setProperty(JdbcConnectionParams.PROPERTY_URL, saveUrl);
    }

    String value = null;
    if (driver != null) {
      props.setProperty(JdbcConnectionParams.PROPERTY_DRIVER, driver);
    } else {
      value = Utils.parsePropertyFromUrl(url, JdbcConnectionParams.PROPERTY_DRIVER);
      if (value != null) {
        props.setProperty(JdbcConnectionParams.PROPERTY_DRIVER, value);
      }
    }

    if (user != null) {
      props.setProperty(JdbcConnectionParams.AUTH_USER, user);
    } else {
      value = Utils.parsePropertyFromUrl(url, JdbcConnectionParams.AUTH_USER);
      if (value != null) {
        props.setProperty(JdbcConnectionParams.AUTH_USER, value);
      }
    }

    if (pass != null) {
      props.setProperty(JdbcConnectionParams.AUTH_PASSWD, pass);
    } else {
      value = Utils.parsePropertyFromUrl(url, JdbcConnectionParams.AUTH_PASSWD);
      if (value != null) {
        props.setProperty(JdbcConnectionParams.AUTH_PASSWD, value);
      }
    }

    value = Utils.parsePropertyFromUrl(url, JdbcConnectionParams.AUTH_TYPE);
    if (value != null) {
      props.setProperty(JdbcConnectionParams.AUTH_TYPE, value);
    }
    return connect(props);
  }

  private String getUrlToUse(String urlParam) {
    boolean useIndirectUrl = false;
    // If the url passed to us is a valid url with a protocol, we use it as-is
    // Otherwise, we assume it is a name of parameter that we have to get the url from
    try {
      URI tryParse = new URI(urlParam);
      if (tryParse.getScheme() == null){
        // param had no scheme, so not a URL
        useIndirectUrl = true;
      }
    } catch (URISyntaxException e){
      // param did not parse as a URL, so not a URL
      useIndirectUrl = true;
    }
    if (useIndirectUrl){
      // Use url param indirectly - as the name of an env var that contains the url
      // If the urlParam is "default", we would look for a BEELINE_URL_DEFAULT url
      String envUrl = beeLine.getOpts().getEnv().get(
        BeeLineOpts.URL_ENV_PREFIX + urlParam.toUpperCase());
      if (envUrl != null){
        return envUrl;
      }
    }
    return urlParam; // default return the urlParam passed in as-is.
  }

  private String getProperty(Properties props, String[] keys) {
    for (int i = 0; i < keys.length; i++) {
      String val = props.getProperty(keys[i]);
      if (val != null) {
        return val;
      }
    }

    for (Iterator i = props.keySet().iterator(); i.hasNext();) {
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
    String url = getProperty(props, new String[] {
      JdbcConnectionParams.PROPERTY_URL,
      "javax.jdo.option.ConnectionURL",
      "ConnectionURL",
    });
    String driver = getProperty(props, new String[] {
      JdbcConnectionParams.PROPERTY_DRIVER,
      "javax.jdo.option.ConnectionDriverName",
      "ConnectionDriverName",
    });
    String username = getProperty(props, new String[] {
      JdbcConnectionParams.AUTH_USER,
      "javax.jdo.option.ConnectionUserName",
      "ConnectionUserName",
    });
    String password = getProperty(props, new String[] {
      JdbcConnectionParams.AUTH_PASSWD,
      "javax.jdo.option.ConnectionPassword",
      "ConnectionPassword",
    });

    if (url == null || url.length() == 0) {
      return beeLine.error("Property \"url\" is required");
    }
    if (driver == null || driver.length() == 0) {
      if (!beeLine.scanForDriver(url)) {
        return beeLine.error(beeLine.loc("no-driver", url));
      }
    }

    String auth = getProperty(props, new String[] {JdbcConnectionParams.AUTH_TYPE});
    if (auth == null) {
      auth = beeLine.getOpts().getAuthType();
      if (auth != null) {
        props.setProperty(JdbcConnectionParams.AUTH_TYPE, auth);
      }
    }

    beeLine.info("Connecting to " + url);
    if (Utils.parsePropertyFromUrl(url, JdbcConnectionParams.AUTH_PRINCIPAL) == null) {
      String urlForPrompt = url.substring(0, url.contains(";") ? url.indexOf(';') : url.length());
      if (username == null) {
        username = beeLine.getConsoleReader().readLine("Enter username for " + urlForPrompt + ": ");
      }
      props.setProperty(JdbcConnectionParams.AUTH_USER, username);
      if (password == null) {
        password = beeLine.getConsoleReader().readLine("Enter password for " + urlForPrompt + ": ",
          new Character('*'));
      }
      props.setProperty(JdbcConnectionParams.AUTH_PASSWD, password);
    }

    try {
      beeLine.getDatabaseConnections().setConnection(
        new KyuubiDatabaseConnection(beeLine, driver, url, props));
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



  /**
   * Run a script from the specified file.
   */
  public boolean run(String line) {
    String[] parts = beeLine.split(line, 2, "Usage: run <scriptfile>");
    if (parts == null) {
      return false;
    }

    List<String> cmds = new LinkedList<String>();

    try {
      BufferedReader reader = new BufferedReader(new FileReader(
        parts[1]));
      try {
        // ### NOTE: fix for sf.net bug 879427
        StringBuilder cmd = null;
        for (;;) {
          String scriptLine = reader.readLine();

          if (scriptLine == null) {
            break;
          }

          String trimmedLine = scriptLine.trim();
          if (beeLine.getOpts().getTrimScripts()) {
            scriptLine = trimmedLine;
          }

          if (cmd != null) {
            // we're continuing an existing command
            cmd.append(" \n");
            cmd.append(scriptLine);
            if (trimmedLine.endsWith(";")) {
              // this command has terminated
              cmds.add(cmd.toString());
              cmd = null;
            }
          } else {
            // we're starting a new command
            if (beeLine.needsContinuation(scriptLine)) {
              // multi-line
              cmd = new StringBuilder(scriptLine);
            } else {
              // single-line
              cmds.add(scriptLine);
            }
          }
        }

        if (cmd != null) {
          // ### REVIEW: oops, somebody left the last command
          // unterminated; should we fix it for them or complain?
          // For now be nice and fix it.
          cmd.append(";");
          cmds.add(cmd.toString());
        }
      } finally {
        reader.close();
      }

      // success only if all the commands were successful
      return beeLine.runCommands(cmds) == cmds.size();
    } catch (Exception e) {
      return beeLine.error(e);
    }
  }


  /**
   * Save or stop saving all output to a file.
   */
  public boolean record(String line) {
    if (beeLine.getRecordOutputFile() == null) {
      return startRecording(line);
    } else {
      return stopRecording(line);
    }
  }


  /**
   * Stop writing output to the record file.
   */
  private boolean stopRecording(String line) {
    try {
      beeLine.getRecordOutputFile().close();
    } catch (Exception e) {
      beeLine.handleException(e);
    }
    beeLine.setRecordOutputFile(null);
    beeLine.output(beeLine.loc("record-closed", beeLine.getRecordOutputFile()));
    return true;
  }


  /**
   * Start writing to the specified record file.
   */
  private boolean startRecording(String line) {
    if (beeLine.getRecordOutputFile() != null) {
      return beeLine.error(beeLine.loc("record-already-running", beeLine.getRecordOutputFile()));
    }

    String[] parts = beeLine.split(line, 2, "Usage: record <filename>");
    if (parts == null) {
      return false;
    }

    try {
      OutputFile recordOutput = new OutputFile(parts[1]);
      beeLine.output(beeLine.loc("record-started", recordOutput));
      beeLine.setRecordOutputFile(recordOutput);
      return true;
    } catch (Exception e) {
      return beeLine.error(e);
    }
  }




  public boolean describe(String line) throws SQLException {
    String[] table = beeLine.split(line, 2, "Usage: describe <table name>");
    if (table == null) {
      return false;
    }

    ResultSet rs;

    if (table[1].equals("tables")) {
      rs = beeLine.getTables();
    } else {
      rs = beeLine.getColumns(table[1]);
    }

    if (rs == null) {
      return false;
    }

    beeLine.print(rs);
    rs.close();
    return true;
  }


  public boolean help(String line) {
    String[] parts = beeLine.split(line);
    String cmd = parts.length > 1 ? parts[1] : "";
    int count = 0;
    TreeSet<ColorBuffer> clist = new TreeSet<ColorBuffer>();

    for (int i = 0; i < beeLine.commandHandlers.length; i++) {
      if (cmd.length() == 0 ||
        Arrays.asList(beeLine.commandHandlers[i].getNames()).contains(cmd)) {
        clist.add(beeLine.getColorBuffer().pad("!" + beeLine.commandHandlers[i].getName(), 20)
          .append(beeLine.wrap(beeLine.commandHandlers[i].getHelpText(), 60, 20)));
      }
    }

    for (Iterator<ColorBuffer> i = clist.iterator(); i.hasNext();) {
      beeLine.output(i.next());
    }

    if (cmd.length() == 0) {
      beeLine.output("");
      beeLine.output(beeLine.loc("comments", beeLine.getApplicationContactInformation()));
    }

    return true;
  }


  public boolean manual(String line) throws IOException {
    InputStream in = BeeLine.class.getResourceAsStream("manual.txt");
    if (in == null) {
      return beeLine.error(beeLine.loc("no-manual"));
    }

    BufferedReader breader = new BufferedReader(
      new InputStreamReader(in));
    String man;
    int index = 0;
    while ((man = breader.readLine()) != null) {
      index++;
      beeLine.output(man);

      // silly little pager
      if (index % (beeLine.getOpts().getMaxHeight() - 1) == 0) {
        String ret = beeLine.getConsoleReader().readLine(beeLine.loc("enter-for-more"));
        if (ret != null && ret.startsWith("q")) {
          break;
        }
      }
    }
    breader.close();
    return true;
  }

  protected Runnable createConnectionLogRunnable(final Connection connection,
                                                 InPlaceUpdateStream.EventNotifier eventNotifier) {
    if (connection instanceof KyuubiConnection) {
      return new KyuubiConnectionLogRunnable(this, (KyuubiConnection) connection,
        DEFAULT_QUERY_PROGRESS_INTERVAL, eventNotifier);
    } else {
      beeLine.debug(
        "The connection instance is not KyuubiConnection type: " + connection.getClass());
      return new Runnable() {
        @Override
        public void run() {
          // do nothing.
        }
      };
    }
  }

  static class KyuubiConnectionLogRunnable implements Runnable {
    private final KyuubiCommands commands;
    private final KyuubiConnection kyuubiConnection;
    private final long queryProgressInterval;
    private final InPlaceUpdateStream.EventNotifier notifier;

    KyuubiConnectionLogRunnable(KyuubiCommands commands, KyuubiConnection kyuubiConnection,
                                long queryProgressInterval,
                                InPlaceUpdateStream.EventNotifier eventNotifier) {
      this.commands = commands;
      this.kyuubiConnection = kyuubiConnection;
      this.queryProgressInterval = queryProgressInterval;
      this.notifier = eventNotifier;
    }

    private void updateQueryLog() {
      try {
        List<String> engineLogs = kyuubiConnection.getEngineLog();
        for (String log : engineLogs) {
          commands.beeLine.info(log);
        }
        if (!engineLogs.isEmpty()) {
          notifier.operationLogShowedToUser();
        }
      } catch (SQLException e) {
        commands.beeLine.error(new SQLWarning(e));
      }
  }

    @Override public void run() {
      try {
        while (kyuubiConnection.hasMoreEngineLogs()) {
          commands.beeLine.debug("going to print launch engine operation logs");
          updateQueryLog();
          commands.beeLine.debug("printed launch engine operation logs");
          Thread.sleep(queryProgressInterval);
        }
      } catch (InterruptedException e) {
        commands.beeLine.debug("Getting log thread is interrupted, since query is done!");
      } finally {
        commands.showRemainingKyuubiEngineLogsIfAny(kyuubiConnection);
      }
    }
  }

  private void showRemainingKyuubiEngineLogsIfAny(KyuubiConnection kyuubiConnection) {
    List<String> logs = null;
    do {
      try {
        logs = kyuubiConnection.getEngineLog();
      } catch (SQLException e) {
        beeLine.error(new SQLWarning(e));
        return;
      }
      for (String log: logs) {
        beeLine.info(log);
      }
    } while (logs.size() > 0);
  }

  static class KyuubiStatementLogRunnable implements Runnable {
    private final KyuubiCommands commands;
    private final KyuubiStatement kyuubiStatement;
    private final long queryProgressInterval;
    private final InPlaceUpdateStream.EventNotifier notifier;

    KyuubiStatementLogRunnable(KyuubiCommands commands, KyuubiStatement kyuubiStatement,
                               long queryProgressInterval,
                               InPlaceUpdateStream.EventNotifier eventNotifier) {
      this.kyuubiStatement = kyuubiStatement;
      this.commands = commands;
      this.queryProgressInterval = queryProgressInterval;
      this.notifier = eventNotifier;
    }

    private void updateQueryLog() {
      try {
        List<String> queryLogs = kyuubiStatement.getQueryLog();
        for (String log : queryLogs) {
          commands.beeLine.info(log);
        }
        if (!queryLogs.isEmpty()) {
          notifier.operationLogShowedToUser();
        }
      } catch (SQLException e) {
        commands.beeLine.error(new SQLWarning(e));
      }
    }

    @Override public void run() {
      try {
        while (kyuubiStatement.hasMoreLogs()) {
          /*
            get the operation logs once and print it, then wait till progress bar update is complete
            before printing the remaining logs.
          */
          if (notifier.canOutputOperationLogs()) {
            commands.beeLine.debug("going to print operations logs");
            updateQueryLog();
            commands.beeLine.debug("printed operations logs");
          }
          Thread.sleep(queryProgressInterval);
        }
      } catch (InterruptedException e) {
        commands.beeLine.debug("Getting log thread is interrupted, since query is done!");
      } finally {
        commands.showRemainingLogsIfAny(kyuubiStatement);
      }
    }
  }
}
