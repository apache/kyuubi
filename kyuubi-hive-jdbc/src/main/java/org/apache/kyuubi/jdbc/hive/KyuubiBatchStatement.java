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
import java.util.*;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KyuubiBatchStatement extends KyuubiStatement {
  private static final Logger LOG = LoggerFactory.getLogger(KyuubiBatchStatement.class.getName());

  private List<String> commands = new ArrayList<>();
  private ResultSet currentResultSet = null;
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
    return currentResultSet;
  }

  public KyuubiStatement getCurrentSubStatement() {
    return currentStatement;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    List<String> queries = splitSemiColon(sql);
    this.commands.addAll(queries);
  }

  @Override
  public void clearBatch() throws SQLException {
    this.commands.clear();
  }

  @Override
  public final int[] executeBatch() throws SQLException {
    if (commands.isEmpty()) {
      LOG.error("No commands to execute, please addBatch before this.");
      return new int[0];
    }
    int[] rets = new int[commands.size()];
    ResultSet currRs = this.currentResultSet;
    KyuubiStatement currStmt = this.currentStatement;
    for (int i = 0; i < commands.size(); i++) {
      KyuubiStatement statement = new KyuubiStatement(connection, client, sessHandle);
      if (statement.execute(commands.get(i))) {
        this.statements.add(statement);
        this.currentResultSet = null;
        rets[i] = Statement.SUCCESS_NO_INFO;
      } else {
        this.statements.add(statement);
        rets[i] = this.getUpdateCount();
      }
    }
    this.currentResultSet = currRs;
    this.currentStatement = currStmt;
    // Make the next available results the current results if there
    // are no current results
    if (this.currentResultSet == null && !this.statements.isEmpty()) {
      KyuubiStatement statement = statements.poll();
      this.currentResultSet = statement.getResultSet();
      this.currentStatement = statement;
    }
    clearBatch();
    return rets;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    if (this.currentStatement != null) {
      closeStatementSilently(this.currentStatement);
      this.currentResultSet = null;
      this.currentStatement = null;
    }
    while (!statements.isEmpty()) {
      KyuubiStatement statement = statements.poll();
      if (statement.getResultSet() != null) {
        this.currentStatement = statement;
        this.currentResultSet = statement.getResultSet();
        return true;
      } else {
        closeStatementSilently(statement);
      }
    }
    return false;
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
    if (currentStatement != null) {
      closeStatementSilently(currentStatement);
      this.currentResultSet = null;
      this.currentStatement = null;
    }

    while (!this.statements.isEmpty()) {
      KyuubiStatement statement = statements.poll();
      closeStatementSilently(statement);
    }
    closeClientOperation();
    client = null;
    isClosed = true;
  }

  /**
   * Copied from Apache Spark SparkSQLCLIDriver::splitSemiColon.
   *
   * @param queries the queries seperated by SemiColon
   * @return the query list
   */
  public static List<String> splitSemiColon(String queries) {
    boolean insideSingleQuote = false;
    boolean insideDoubleQuote = false;
    boolean insideSimpleComment = false;
    int bracketedCommentLevel = 0;
    boolean escape = false;
    int beginIndex = 0;
    boolean leavingBracketedComment = false;
    boolean isStatement = false;
    ArrayList<String> ret = new ArrayList<>();

    for (int index = 0; index < queries.length(); index++) {
      // Checks if we need to decrement a bracketed comment level; the last character '/' of
      // bracketed comments is still inside the comment, so `insideBracketedComment` must keep true
      // in the previous loop and we decrement the level here if needed.
      if (leavingBracketedComment) {
        bracketedCommentLevel -= 1;
        leavingBracketedComment = false;
      }

      if (queries.charAt(index) == '\'' && !(insideSimpleComment || bracketedCommentLevel > 0)) {
        // take a look to see if it is escaped
        // See the comment above about SPARK-31595
        if (!escape && !insideDoubleQuote) {
          // flip the boolean variable
          insideSingleQuote = !insideSingleQuote;
        }
      } else if (queries.charAt(index) == '\"'
          && !(insideSimpleComment || bracketedCommentLevel > 0)) {
        // take a look to see if it is escaped
        // See the comment above about SPARK-31595
        if (!escape && !insideSingleQuote) {
          // flip the boolean variable
          insideDoubleQuote = !insideDoubleQuote;
        }
      } else if (queries.charAt(index) == '-') {
        boolean hasNext = index + 1 < queries.length();
        if (insideDoubleQuote
            || insideSingleQuote
            || (insideSimpleComment || bracketedCommentLevel > 0)) {
          // Ignores '-' in any case of quotes or comment.
          // Avoids to start a comment(--) within a quoted segment or already in a comment.
          // Sample query: select "quoted value --"
          //                                    ^^ avoids starting a comment if it's inside quotes.
        } else if (hasNext && queries.charAt(index + 1) == '-') {
          // ignore quotes and ; in simple comment
          insideSimpleComment = true;
        }
      } else if (queries.charAt(index) == ';') {
        if (insideSingleQuote
            || insideDoubleQuote
            || (insideSimpleComment || bracketedCommentLevel > 0)) {
          // do not split
        } else {
          if (isStatement) {
            // split, do not include ; itself
            ret.add(queries.substring(beginIndex, index));
          }
          beginIndex = index + 1;
          isStatement = false;
        }
      } else if (queries.charAt(index) == '\n') {
        // with a new line the inline simple comment should end.
        if (!escape) {
          insideSimpleComment = false;
        }
      } else if (queries.charAt(index) == '/' && !insideSimpleComment) {
        boolean hasNext = index + 1 < queries.length();
        if (insideSingleQuote || insideDoubleQuote) {
          // Ignores '/' in any case of quotes
        } else if ((bracketedCommentLevel > 0) && queries.charAt(index - 1) == '*') {
          // Decrements `bracketedCommentLevel` at the beginning of the next loop
          leavingBracketedComment = true;
        } else if (hasNext && queries.charAt(index + 1) == '*') {
          bracketedCommentLevel += 1;
        }
      }
      // set the escape
      if (escape) {
        escape = false;
      } else if (queries.charAt(index) == '\\') {
        escape = true;
      }

      isStatement =
          isStatement
              || (!(insideSimpleComment || bracketedCommentLevel > 0)
                  && index > beginIndex
                  && !("" + queries.charAt(index)).trim().isEmpty());
    }
    // Check the last char is end of nested bracketed comment.
    boolean endOfBracketedComment = leavingBracketedComment && bracketedCommentLevel == 1;
    // Spark SQL support simple comment and nested bracketed comment in query body.
    // But if Spark SQL receives a comment alone, it will throw parser exception.
    // In Spark SQL CLI, if there is a completed comment in the end of whole query,
    // since Spark SQL CLL use `;` to split the query, CLI will pass the comment
    // to the backend engine and throw exception. CLI should ignore this comment,
    // If there is an uncompleted statement or an uncompleted bracketed comment in the end,
    // CLI should also pass this part to the backend engine, which may throw an exception
    // with clear error message.
    if (!endOfBracketedComment && (isStatement || (bracketedCommentLevel > 0))) {
      ret.add(queries.substring(beginIndex));
    }
    return ret;
  }
}
