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

import org.apache.kyuubi.jdbc.hive.KyuubiConnection;

import org.apache.kyuubi.jdbc.hive.KyuubiStatement;
import org.apache.kyuubi.jdbc.hive.logs.InPlaceUpdateStream;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

public class KyuubiCommands extends Commands {
  private static final int DEFAULT_QUERY_PROGRESS_INTERVAL = 1000;
  private static final int DEFAULT_QUERY_PROGRESS_THREAD_TIMEOUT = 10 * 1000;

  protected KyuubiBeeLine beeLine;
  public KyuubiCommands(KyuubiBeeLine beeLine) {
    super(beeLine);
    this.beeLine = beeLine;
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
        commands.showRemainingKyuubiStatementLogsIfAny(kyuubiStatement);
      }
    }
  }

  private void showRemainingKyuubiStatementLogsIfAny(KyuubiStatement kyuubiStatement) {
    List<String> logs = null;
    do {
      try {
        logs = kyuubiStatement.getQueryLog();
      } catch (SQLException e) {
        beeLine.error(new SQLWarning(e));
        return;
      }
      for (String log: logs) {
        beeLine.info(log);
      }
    } while (logs.size() > 0);
  }
}
