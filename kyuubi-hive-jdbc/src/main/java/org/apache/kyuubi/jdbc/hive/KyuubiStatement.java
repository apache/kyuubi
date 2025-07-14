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

import com.google.common.annotations.VisibleForTesting;
import java.sql.*;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.jdbc.hive.adapter.SQLStatement;
import org.apache.kyuubi.jdbc.hive.cli.FetchType;
import org.apache.kyuubi.jdbc.hive.cli.RowSet;
import org.apache.kyuubi.jdbc.hive.cli.RowSetFactory;
import org.apache.kyuubi.jdbc.hive.logs.InPlaceUpdateStream;
import org.apache.kyuubi.jdbc.hive.logs.KyuubiLoggable;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.*;
import org.apache.kyuubi.shaded.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** KyuubiStatement. */
public class KyuubiStatement implements SQLStatement, KyuubiLoggable {
  public static final Logger LOG = LoggerFactory.getLogger(KyuubiStatement.class.getName());
  public static final int DEFAULT_FETCH_SIZE = 1000;
  public static final String DEFAULT_RESULT_FORMAT = "thrift";
  public static final String DEFAULT_ARROW_TIMESTAMP_AS_STRING = "false";
  private final KyuubiConnection connection;
  private TCLIService.Iface client;
  private volatile TOperationHandle stmtHandle = null;
  // This lock must be acquired before modifying or judge stmt
  // to ensure there are no concurrent accesses or race conditions.
  private final Lock stmtHandleAccessLock = new ReentrantLock();
  private final TSessionHandle sessHandle;
  Map<String, String> sessConf = new HashMap<>();
  private int fetchSize = DEFAULT_FETCH_SIZE;
  private boolean isScrollableResultset = false;
  private boolean isOperationComplete = false;

  private Map<String, String> properties = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
  /**
   * We need to keep a reference to the result set to support the following: <code>
   * statement.execute(String sql);
   * statement.getResultSet();
   * </code>.
   */
  private ResultSet resultSet = null;

  /**
   * Sets the limit for the maximum number of rows that any ResultSet object produced by this
   * Statement can contain to the given number. If the limit is exceeded, the excess rows are
   * silently dropped. The value must be >= 0, and 0 means there is not limit.
   */
  private int maxRows = 0;

  /** Add SQLWarnings to the warningChain if needed. */
  private SQLWarning warningChain = null;

  /** Keep state so we can fail certain calls made after close(). */
  private volatile boolean isClosed = false;

  /** Keep state so we can fail certain calls made after cancel(). */
  private volatile boolean isCancelled = false;

  /** Keep this state so we can know whether the query in this statement is closed. */
  private boolean isQueryClosed = false;

  /** Keep this state so we can know whether the query logs are being generated in HS2. */
  private boolean isLogBeingGenerated = true;

  /**
   * Keep this state so we can know whether the statement is submitted to HS2 and start execution
   * successfully.
   */
  private boolean isExecuteStatementFailed = false;

  private int queryTimeout = 0;

  private InPlaceUpdateStream inPlaceUpdateStream = InPlaceUpdateStream.NO_OP;

  public KyuubiStatement(
      KyuubiConnection connection, TCLIService.Iface client, TSessionHandle sessHandle) {
    this(connection, client, sessHandle, false, DEFAULT_FETCH_SIZE);
  }

  public KyuubiStatement(
      KyuubiConnection connection,
      TCLIService.Iface client,
      TSessionHandle sessHandle,
      int fetchSize) {
    this(connection, client, sessHandle, false, fetchSize);
  }

  public KyuubiStatement(
      KyuubiConnection connection,
      TCLIService.Iface client,
      TSessionHandle sessHandle,
      boolean isScrollableResultset) {
    this(connection, client, sessHandle, isScrollableResultset, DEFAULT_FETCH_SIZE);
  }

  public KyuubiStatement(
      KyuubiConnection connection,
      TCLIService.Iface client,
      TSessionHandle sessHandle,
      boolean isScrollableResultset,
      int fetchSize) {
    this.connection = connection;
    this.client = client;
    this.sessHandle = sessHandle;
    this.isScrollableResultset = isScrollableResultset;
    this.fetchSize = fetchSize;
  }

  @Override
  public void cancel() throws SQLException {
    try {
      stmtHandleAccessLock.lock();
      checkConnection("cancel");
      if (isCancelled) {
        return;
      }
      if (stmtHandle != null) {
        TCancelOperationReq cancelReq = new TCancelOperationReq(stmtHandle);
        TCancelOperationResp cancelResp = client.CancelOperation(cancelReq);
        Utils.verifySuccessWithInfo(cancelResp.getStatus());
      }
      isCancelled = true;
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new KyuubiSQLException(e.toString(), "08S01", e);
    } finally {
      stmtHandleAccessLock.unlock();
    }
  }

  @Override
  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  /**
   * Closes the statement if there is one running. Do not change the the flags.
   *
   * @throws SQLException If there is an error closing the statement
   */
  private void closeStatementIfNeeded() throws SQLException {
    try {
      if (stmtHandle != null) {
        TCloseOperationReq closeReq = new TCloseOperationReq(stmtHandle);
        TCloseOperationResp closeResp = client.CloseOperation(closeReq);
        Utils.verifySuccessWithInfo(closeResp.getStatus());
        stmtHandle = null;
      }
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new KyuubiSQLException(e.toString(), "08S01", e);
    }
  }

  void closeClientOperation() throws SQLException {
    closeStatementIfNeeded();
    isQueryClosed = true;
    isExecuteStatementFailed = false;
    stmtHandle = null;
  }

  @Override
  public void close() throws SQLException {
    try {
      stmtHandleAccessLock.lock();
      if (isClosed) {
        return;
      }
      closeClientOperation();
      client = null;
      closeResultSet();
      isClosed = true;
    } finally {
      stmtHandleAccessLock.unlock();
    }
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    return executeWithConfOverlay(sql, null);
  }

  private boolean executeWithConfOverlay(String sql, Map<String, String> confOverlay)
      throws SQLException {
    runAsyncOnServer(sql, confOverlay);
    TGetOperationStatusResp status = waitForOperationToComplete();

    // The query should be completed by now
    if (!status.isHasResultSet() && !stmtHandle.isHasResultSet()) {
      return false;
    }

    TGetResultSetMetadataResp metadata = getResultSetMetadata();
    List<String> columnNames = new ArrayList<>();
    List<TTypeId> columnTypes = new ArrayList<>();
    List<JdbcColumnAttributes> columnAttributes = new ArrayList<>();
    parseMetadata(metadata, columnNames, columnTypes, columnAttributes);

    String resultFormat =
        properties.getOrDefault("__kyuubi_operation_result_format__", DEFAULT_RESULT_FORMAT);
    LOG.debug("kyuubi.operation.result.format: {}", resultFormat);
    switch (resultFormat) {
      case "arrow":
        boolean timestampAsString =
            Boolean.parseBoolean(
                properties.getOrDefault(
                    "__kyuubi_operation_result_arrow_timestampAsString__",
                    DEFAULT_ARROW_TIMESTAMP_AS_STRING));
        resultSet =
            new KyuubiArrowQueryResultSet.Builder(this)
                .setClient(client)
                .setSessionHandle(sessHandle)
                .setStmtHandle(stmtHandle)
                .setMaxRows(maxRows)
                .setFetchSize(fetchSize)
                .setScrollable(isScrollableResultset)
                .setSchema(columnNames, columnTypes, columnAttributes)
                .setTimestampAsString(timestampAsString)
                .build();
        break;
      default:
        resultSet =
            new KyuubiQueryResultSet.Builder(this)
                .setClient(client)
                .setSessionHandle(sessHandle)
                .setStmtHandle(stmtHandle)
                .setMaxRows(maxRows)
                .setFetchSize(fetchSize)
                .setScrollable(isScrollableResultset)
                .setSchema(columnNames, columnTypes, columnAttributes)
                .build();
    }
    return true;
  }

  /**
   * Starts the query execution asynchronously on the server, and immediately returns to the client.
   * The client subsequently blocks on ResultSet#next or Statement#getUpdateCount, depending on the
   * query type. Users should call ResultSet.next or Statement#getUpdateCount (depending on whether
   * query returns results) to ensure that query completes successfully. Calling another execute*
   * method, or close before query completion would result in the async query getting killed if it
   * is not already finished. Note: This method is an API for limited usage outside of Hive by
   * applications like Apache Ambari, although it is not part of the interface java.sql.Statement.
   *
   * @param sql
   * @return true if the first result is a ResultSet object; false if it is an update count or there
   *     are no results
   * @throws SQLException
   */
  public boolean executeAsync(String sql) throws SQLException {
    runAsyncOnServer(sql);
    TGetOperationStatusResp status = waitForResultSetStatus();
    if (!status.isHasResultSet()) {
      return false;
    }
    TGetResultSetMetadataResp metadata = getResultSetMetadata();
    List<String> columnNames = new ArrayList<>();
    List<TTypeId> columnTypes = new ArrayList<>();
    List<JdbcColumnAttributes> columnAttributes = new ArrayList<>();
    parseMetadata(metadata, columnNames, columnTypes, columnAttributes);

    String resultFormat =
        properties.getOrDefault("__kyuubi_operation_result_format__", DEFAULT_RESULT_FORMAT);
    LOG.debug("kyuubi.operation.result.format: {}", resultFormat);
    switch (resultFormat) {
      case "arrow":
        boolean timestampAsString =
            Boolean.parseBoolean(
                properties.getOrDefault(
                    "__kyuubi_operation_result_arrow_timestampAsString__",
                    DEFAULT_ARROW_TIMESTAMP_AS_STRING));
        resultSet =
            new KyuubiArrowQueryResultSet.Builder(this)
                .setClient(client)
                .setSessionHandle(sessHandle)
                .setStmtHandle(stmtHandle)
                .setMaxRows(maxRows)
                .setFetchSize(fetchSize)
                .setScrollable(isScrollableResultset)
                .setSchema(columnNames, columnTypes, columnAttributes)
                .setTimestampAsString(timestampAsString)
                .build();
        break;
      default:
        resultSet =
            new KyuubiQueryResultSet.Builder(this)
                .setClient(client)
                .setSessionHandle(sessHandle)
                .setStmtHandle(stmtHandle)
                .setMaxRows(maxRows)
                .setFetchSize(fetchSize)
                .setScrollable(isScrollableResultset)
                .setSchema(columnNames, columnTypes, columnAttributes)
                .build();
    }
    return true;
  }

  private void runAsyncOnServer(String sql) throws SQLException {
    runAsyncOnServer(sql, null);
  }

  private void runAsyncOnServer(String sql, Map<String, String> confOneTime) throws SQLException {
    stmtHandleAccessLock.lock();
    try {
      checkConnection("execute");

      reInitState();

      TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, sql);
      /**
       * Run asynchronously whenever possible Currently only a SQLOperation can be run
       * asynchronously, in a background operation thread Compilation can run asynchronously or
       * synchronously and execution run asynchronously
       */
      execReq.setRunAsync(true);
      if (confOneTime != null) {
        Map<String, String> confOverlay = new HashMap<>(sessConf);
        confOverlay.putAll(confOneTime);
        execReq.setConfOverlay(confOverlay);
      } else {
        execReq.setConfOverlay(sessConf);
      }
      execReq.setQueryTimeout(queryTimeout);
      try {
        TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
        Utils.verifySuccessWithInfo(execResp.getStatus());
        stmtHandle = execResp.getOperationHandle();
        isExecuteStatementFailed = false;
      } catch (SQLException eS) {
        isExecuteStatementFailed = true;
        isLogBeingGenerated = false;
        throw eS;
      } catch (Exception ex) {
        isExecuteStatementFailed = true;
        isLogBeingGenerated = false;
        throw new KyuubiSQLException(ex.toString(), "08S01", ex);
      }
    } finally {
      stmtHandleAccessLock.unlock();
    }
  }

  /**
   * Poll the result set status by checking if isSetHasResultSet is set
   *
   * @return
   * @throws SQLException
   */
  private TGetOperationStatusResp waitForResultSetStatus() throws SQLException {
    TGetOperationStatusReq statusReq = new TGetOperationStatusReq(stmtHandle);
    TGetOperationStatusResp statusResp = null;

    while (statusResp == null || !statusResp.isSetHasResultSet()) {
      try {
        statusResp = client.GetOperationStatus(statusReq);
      } catch (TException e) {
        isLogBeingGenerated = false;
        throw new KyuubiSQLException(e.toString(), "08S01", e);
      }
    }

    return statusResp;
  }

  TGetOperationStatusResp waitForOperationToComplete() throws SQLException {
    TGetOperationStatusReq statusReq = new TGetOperationStatusReq(stmtHandle);
    boolean shouldGetProgressUpdate = inPlaceUpdateStream != InPlaceUpdateStream.NO_OP;
    statusReq.setGetProgressUpdate(shouldGetProgressUpdate);
    if (!shouldGetProgressUpdate) {
      /** progress bar is completed if there is nothing we want to request in the first place. */
      inPlaceUpdateStream.getEventNotifier().progressBarCompleted();
    }
    TGetOperationStatusResp statusResp = null;

    // Poll on the operation status, till the operation is complete
    while (!isOperationComplete) {
      try {
        /**
         * For an async SQLOperation, GetOperationStatus will use the long polling approach It will
         * essentially return after the HIVE_SERVER2_LONG_POLLING_TIMEOUT (a server config) expires
         */
        statusResp = client.GetOperationStatus(statusReq);
        inPlaceUpdateStream.update(statusResp.getProgressUpdateResponse());
        Utils.verifySuccessWithInfo(statusResp.getStatus());
        if (statusResp.isSetOperationState()) {
          switch (statusResp.getOperationState()) {
            case CLOSED_STATE:
            case FINISHED_STATE:
              isOperationComplete = true;
              isLogBeingGenerated = false;
              break;
            case CANCELED_STATE:
              // 01000 -> warning
              String errMsg = statusResp.getErrorMessage();
              if (errMsg != null && !errMsg.isEmpty()) {
                throw new KyuubiSQLException("Query was cancelled. " + errMsg, "01000");
              } else {
                throw new KyuubiSQLException("Query was cancelled", "01000");
              }
            case TIMEDOUT_STATE:
              throw new SQLTimeoutException("Query timed out after " + queryTimeout + " seconds");
            case ERROR_STATE:
              // Get the error details from the underlying exception
              throw new KyuubiSQLException(
                  statusResp.getErrorMessage(),
                  statusResp.getSqlState(),
                  statusResp.getErrorCode());
            case UKNOWN_STATE:
              throw new KyuubiSQLException("Unknown query", "HY000");
            case INITIALIZED_STATE:
            case PENDING_STATE:
            case RUNNING_STATE:
              break;
          }
        }
      } catch (SQLException e) {
        isLogBeingGenerated = false;
        throw e;
      } catch (Exception e) {
        isLogBeingGenerated = false;
        throw new KyuubiSQLException(e.toString(), "08S01", e);
      }
    }

    /*
      we set progress bar to be completed when hive query execution has completed
    */
    inPlaceUpdateStream.getEventNotifier().progressBarCompleted();
    return statusResp;
  }

  private void checkConnection(String action) throws SQLException {
    if (isClosed) {
      throw new KyuubiSQLException("Can't " + action + " after statement has been closed");
    }
  }

  /**
   * Close statement if needed, and reset the flags.
   *
   * @throws SQLException
   */
  private void reInitState() throws SQLException {
    closeStatementIfNeeded();
    isCancelled = false;
    isQueryClosed = false;
    isLogBeingGenerated = true;
    isExecuteStatementFailed = false;
    isOperationComplete = false;
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    if (!execute(sql)) {
      throw new KyuubiSQLException("The query did not generate a result set!");
    }
    return resultSet;
  }

  public ResultSet executeScala(String code) throws SQLException {
    if (!executeWithConfOverlay(
        code, Collections.singletonMap("kyuubi.operation.language", "SCALA"))) {
      throw new KyuubiSQLException("The query did not generate a result set!");
    }
    return resultSet;
  }

  public ResultSet executePython(String code) throws SQLException {
    if (!executeWithConfOverlay(
        code, Collections.singletonMap("kyuubi.operation.language", "PYTHON"))) {
      throw new KyuubiSQLException("The query did not generate a result set!");
    }
    return resultSet;
  }

  public void executeSetCurrentCatalog(String sql, String catalog) throws SQLException {
    if (executeWithConfOverlay(
        sql, Collections.singletonMap("kyuubi.operation.set.current.catalog", catalog))) {
      closeResultSet();
    }
  }

  public ResultSet executeGetCurrentCatalog(String sql) throws SQLException {
    if (!executeWithConfOverlay(
        sql, Collections.singletonMap("kyuubi.operation.get.current.catalog", ""))) {
      throw new KyuubiSQLException("The query did not generate a result set!");
    }
    return resultSet;
  }

  public void executeSetCurrentDatabase(String sql, String database) throws SQLException {
    if (executeWithConfOverlay(
        sql, Collections.singletonMap("kyuubi.operation.set.current.database", database))) {
      closeResultSet();
    }
  }

  public ResultSet executeGetCurrentDatabase(String sql) throws SQLException {
    if (!executeWithConfOverlay(
        sql, Collections.singletonMap("kyuubi.operation.get.current.database", ""))) {
      throw new KyuubiSQLException("The query did not generate a result set!");
    }
    return resultSet;
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    execute(sql);
    return 0;
  }

  @Override
  public Connection getConnection() throws SQLException {
    checkConnection("getConnection");
    return this.connection;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    checkConnection("getFetchDirection");
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public int getFetchSize() throws SQLException {
    checkConnection("getFetchSize");
    return fetchSize;
  }

  @Override
  public int getMaxRows() throws SQLException {
    checkConnection("getMaxRows");
    return maxRows;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    if (current == Statement.CLOSE_CURRENT_RESULT) {
      closeResultSet();
      return false;
    }

    if (current != KEEP_CURRENT_RESULT && current != CLOSE_ALL_RESULTS) {
      throw new SQLException("Invalid argument: " + current);
    }

    throw new SQLFeatureNotSupportedException("Multiple open results not supported");
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    // The javadoc of this method says, that it should implicitly close any current
    // ResultSet object, i.e. is similar to calling the getMoreResults(int)
    // method with Statement.CLOSE_CURRENT_RESULT argument.
    // this is an additional enhancement on top of HIVE-7680
    return getMoreResults(Statement.CLOSE_CURRENT_RESULT);
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    checkConnection("getQueryTimeout");
    return 0;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    checkConnection("getResultSet");
    return resultSet;
  }

  @Override
  public int getResultSetType() throws SQLException {
    checkConnection("getResultSetType");
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    checkConnection("getUpdateCount");
    /**
     * Poll on the operation status, till the operation is complete. We want to ensure that since a
     * client might end up using executeAsync and then call this to check if the query run is
     * finished.
     */
    waitForOperationToComplete();
    return -1;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    checkConnection("getWarnings");
    return warningChain;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return false;
  }

  @Override
  public boolean isPoolable() throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    checkConnection("setFetchDirection");
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new KyuubiSQLException("Not supported direction " + direction);
    }
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    checkConnection("setFetchSize");
    if (rows > 0) {
      fetchSize = rows;
    } else if (rows == 0) {
      // Javadoc for Statement interface states that if the value is zero
      // then "fetch size" hint is ignored.
      // In this case it means reverting it to the default value.
      fetchSize = DEFAULT_FETCH_SIZE;
    } else {
      throw new KyuubiSQLException("Fetch size must be greater or equal to 0");
    }
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    checkConnection("setMaxRows");
    if (max < 0) {
      throw new KyuubiSQLException("max must be >= 0");
    }
    maxRows = max;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    this.queryTimeout = seconds;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new KyuubiSQLException("Cannot unwrap to " + iface);
  }

  /**
   * Check whether query execution might be producing more logs to be fetched. This method is a
   * public API for usage outside of Hive, although it is not part of the interface
   * java.sql.Statement.
   *
   * @return true if query execution might be producing more logs. It does not indicate if last log
   *     lines have been fetched by getQueryLog.
   */
  @Override
  public boolean hasMoreLogs() {
    return isLogBeingGenerated;
  }

  /**
   * Get the execution logs of the given SQL statement. This method is a public API for usage
   * outside of Hive, although it is not part of the interface java.sql.Statement. This method gets
   * the incremental logs during SQL execution, and uses fetchSize holden by HiveStatement object.
   *
   * @return a list of logs. It can be empty if there are no new logs to be retrieved at that time.
   * @throws SQLException
   * @throws ClosedOrCancelledException if statement has been cancelled or closed
   */
  @Override
  public List<String> getExecLog() throws SQLException, ClosedOrCancelledException {
    return getQueryLog(true, fetchSize);
  }

  /**
   * Get the execution logs of the given SQL statement. This method is a public API for usage
   * outside of Hive, although it is not part of the interface java.sql.Statement.
   *
   * @param incremental indicate getting logs either incrementally or from the beginning, when it is
   *     true or false.
   * @param fetchSize the number of lines to fetch
   * @return a list of logs. It can be empty if there are no new logs to be retrieved at that time.
   * @throws SQLException
   * @throws ClosedOrCancelledException if statement has been cancelled or closed
   */
  public List<String> getQueryLog(boolean incremental, int fetchSize)
      throws SQLException, ClosedOrCancelledException {
    checkConnection("getQueryLog");
    if (isCancelled) {
      throw new ClosedOrCancelledException(
          "Method getQueryLog() failed. The " + "statement has been closed or cancelled.");
    }

    List<String> logs = new ArrayList<String>();
    TFetchResultsResp tFetchResultsResp = null;
    try {
      if (stmtHandle != null) {
        TFetchResultsReq tFetchResultsReq =
            new TFetchResultsReq(stmtHandle, getFetchOrientation(incremental), fetchSize);
        tFetchResultsReq.setFetchType(FetchType.LOG.toTFetchType());
        tFetchResultsResp = client.FetchResults(tFetchResultsReq);
        Utils.verifySuccessWithInfo(tFetchResultsResp.getStatus());
      } else {
        if (isQueryClosed) {
          throw new ClosedOrCancelledException(
              "Method getQueryLog() failed. The " + "statement has been closed or cancelled.");
        } else {
          return logs;
        }
      }
    } catch (SQLException e) {
      throw e;
    } catch (TException e) {
      throw new KyuubiSQLException("Error when getting query log: " + e, e);
    } catch (Exception e) {
      throw new KyuubiSQLException("Error when getting query log: " + e, e);
    }

    try {
      RowSet rowSet;
      rowSet = RowSetFactory.create(tFetchResultsResp.getResults(), connection.getProtocol());
      for (Object[] row : rowSet) {
        logs.add(String.valueOf(row[0]));
      }
    } catch (TException e) {
      throw new KyuubiSQLException("Error building result set for query log: " + e, e);
    }

    return logs;
  }

  private TFetchOrientation getFetchOrientation(boolean incremental) {
    if (incremental) {
      return TFetchOrientation.FETCH_NEXT;
    } else {
      return TFetchOrientation.FETCH_FIRST;
    }
  }

  /**
   * Returns the Yarn ATS GUID. This method is a public API for usage outside of Hive, although it
   * is not part of the interface java.sql.Statement.
   *
   * @return Yarn ATS GUID or null if it hasn't been created yet.
   */
  public String getYarnATSGuid() {
    if (stmtHandle != null) {
      // Set on the server side.
      // @see org.apache.hive.service.cli.operation.SQLOperation#prepare
      String guid64 =
          Base64.getUrlEncoder().encodeToString(stmtHandle.getOperationId().getGuid()).trim();
      return guid64;
    }
    return null;
  }

  /**
   * Returns the Query ID if it is running. This method is a public API for usage outside of Hive,
   * although it is not part of the interface java.sql.Statement.
   *
   * @return Valid query ID if it is running else returns NULL.
   * @throws SQLException If any internal failures.
   */
  @VisibleForTesting
  public String getQueryId() throws SQLException {
    if (stmtHandle == null) {
      // If query is not running or already closed.
      return null;
    }

    try {
      final String queryId = client.GetQueryId(new TGetQueryIdReq(stmtHandle)).getQueryId();

      // queryId can be empty string if query was already closed. Need to return null in such case.
      return StringUtils.isBlank(queryId) ? null : queryId;
    } catch (TException e) {
      throw new KyuubiSQLException(e);
    }
  }

  /**
   * This is only used by the beeline client to set the stream on which in place progress updates
   * are to be shown
   */
  public void setInPlaceUpdateStream(InPlaceUpdateStream stream) {
    this.inPlaceUpdateStream = stream;
  }

  private TGetResultSetMetadataResp getResultSetMetadata() throws SQLException {
    try {
      TGetResultSetMetadataReq metadataReq = new TGetResultSetMetadataReq(stmtHandle);
      // TODO need session handle
      TGetResultSetMetadataResp metadataResp;
      metadataResp = client.GetResultSetMetadata(metadataReq);
      Utils.verifySuccess(metadataResp.getStatus());
      return metadataResp;
    } catch (SQLException eS) {
      throw eS; // rethrow the SQLException as is
    } catch (Exception ex) {
      throw new KyuubiSQLException("Could not create ResultSet: " + ex.getMessage(), ex);
    }
  }

  private void parseMetadata(
      TGetResultSetMetadataResp metadataResp,
      List<String> columnNames,
      List<TTypeId> columnTypes,
      List<JdbcColumnAttributes> columnAttributes)
      throws KyuubiSQLException {
    TTableSchema schema = metadataResp.getSchema();
    if (schema == null || !schema.isSetColumns()) {
      throw new KyuubiSQLException("the result set schema is null");
    }

    // parse kyuubi hint
    List<String> infoMessages = metadataResp.getStatus().getInfoMessages();
    if (infoMessages != null) {
      metadataResp.getStatus().getInfoMessages().stream()
          .filter(hint -> Utils.isKyuubiOperationHint(hint))
          .forEach(
              line -> {
                String[] keyValue = line.toLowerCase(Locale.ROOT).split("=");
                assert keyValue.length == 2 : "Illegal Kyuubi operation hint found!";
                String key = keyValue[0];
                String value = keyValue[1];
                properties.put(key, value);
              });
    }

    // parse metadata
    List<TColumnDesc> columns = schema.getColumns();
    for (int pos = 0; pos < schema.getColumnsSize(); pos++) {
      String columnName = columns.get(pos).getColumnName();
      columnNames.add(columnName);
      TPrimitiveTypeEntry primitiveTypeEntry =
          columns.get(pos).getTypeDesc().getTypes().get(0).getPrimitiveEntry();
      columnTypes.add(primitiveTypeEntry.getType());
      columnAttributes.add(KyuubiArrowQueryResultSet.getColumnAttributes(primitiveTypeEntry));
    }
  }

  private void closeResultSet() throws SQLException {
    if (resultSet != null) {
      resultSet.close();
      resultSet = null;
    }
  }
}
