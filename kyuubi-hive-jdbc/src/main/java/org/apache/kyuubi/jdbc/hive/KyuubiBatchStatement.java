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

package org.apache.kyuubi.jdbc.hive;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KyuubiBatchStatement extends KyuubiStatement {
  private static final Logger LOG = LoggerFactory.getLogger(KyuubiBatchStatement.class.getName());

  private List<String> commands = new ArrayList<>();
  private ResultSet currResultSet = null;
  private KyuubiStatement currentStatement = null;
  private Queue<KyuubiStatement> statements = new LinkedList<>();

  public KyuubiBatchStatement(
      KyuubiConnection connection,
      TCLIService.Iface client,
      TSessionHandle sessHandle,
      int fetchSize) {
    super(connection, client, sessHandle, fetchSize);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return currResultSet;
  }

  public KyuubiStatement getCurrentSubStatement() {
    return currentStatement;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    this.commands.add(sql);
  }

  @Override
  public void clearBatch() throws SQLException {
    this.commands.clear();
  }

  @Override
  public final int[] executeBatch() throws SQLException {
    // Issue warning where appropriate
    if (commands.size() > 1) {
      LOG.warn("Executing the batch of commands " + String.join(";\n", commands));
    }
    int[] rets = new int[commands.size()];
    ResultSet curRs = this.currResultSet;
    KyuubiStatement curStmt = this.currentStatement;
    for (int i = 0; i < commands.size(); i++) {
      KyuubiStatement statement = new KyuubiStatement(connection, client, sessHandle);
      if (statement.execute(commands.get(i))) {
        this.statements.add(statement);
        this.currResultSet = null;
        rets[i] = Statement.SUCCESS_NO_INFO;
      } else {
        // Need to add a null to getMoreResults() to produce correct
        // behavior across subsequent calls to getMoreResults()
        this.statements.add(statement);
        rets[i] = this.getUpdateCount();
      }
    }
    this.currResultSet = curRs;
    this.currentStatement = curStmt;
    // Make the next available results the current results if there
    // are no current results
    if (this.currResultSet == null && !this.statements.isEmpty()) {
      KyuubiStatement statement = statements.poll();
      this.currResultSet = statement.getResultSet();
      this.currentStatement = statement;
    }
    clearBatch();
    return rets;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    if (this.currentStatement != null) {
      closeStatementSilently(this.currentStatement);
      this.currResultSet = null;
      this.currentStatement = null;
    }
    if (!this.statements.isEmpty()) {
      KyuubiStatement statement = statements.poll();
      this.currResultSet = statement.getResultSet();
      this.currentStatement = statement;
      return true;
    } else {
      return false;
    }
  }

  private void closeStatementSilently(KyuubiStatement statement) {
    try {
      statement.close();
    } catch (Exception e) {
      LOG.error("Error closing statement:" + statement.getYarnATSGuid(), e);
    }
  }

  @Override
  public void close() throws SQLException {
    if (isClosed) {
      return;
    }
    closeClientOperation();
    client = null;
    try {
      if (currentStatement != null) {
        closeStatementSilently(currentStatement);
        this.currResultSet = null;
        this.currentStatement = null;
      }
    } catch (Exception e) {

    }

    while (!this.statements.isEmpty()) {
      KyuubiStatement statement = statements.poll();
      closeStatementSilently(statement);
    }
    isClosed = true;
  }
}
