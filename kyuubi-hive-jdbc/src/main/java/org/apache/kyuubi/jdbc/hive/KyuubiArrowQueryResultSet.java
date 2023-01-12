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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.kyuubi.jdbc.hive.arrow.ArrowColumnVector;
import org.apache.kyuubi.jdbc.hive.arrow.ArrowColumnarBatch;
import org.apache.kyuubi.jdbc.hive.arrow.ArrowColumnarBatchRow;
import org.apache.kyuubi.jdbc.hive.arrow.ArrowUtils;
import org.apache.kyuubi.jdbc.hive.common.HiveDecimal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KyuubiArrowQueryResultSet extends KyuubiArrowBasedResultSet {

  public static final Logger LOG = LoggerFactory.getLogger(KyuubiArrowQueryResultSet.class);

  private TCLIService.Iface client;
  private TOperationHandle stmtHandle;
  private TSessionHandle sessHandle;

  private int maxRows;
  private int fetchSize;
  private int rowsFetched = 0;

  private Iterator<ArrowColumnarBatchRow> fetchedRowsItr;
  private boolean isClosed = false;
  private boolean emptyResultSet = false;
  private boolean isScrollable = false;
  private boolean fetchFirst = false;

  // TODO:(fchen) make this configurable
  protected boolean convertComplexTypeToString = true;

  private final TProtocolVersion protocol;

  public static class Builder {

    private final Connection connection;
    private final Statement statement;
    private TCLIService.Iface client = null;
    private TOperationHandle stmtHandle = null;
    private TSessionHandle sessHandle = null;

    /**
     * Sets the limit for the maximum number of rows that any ResultSet object produced by this
     * Statement can contain to the given number. If the limit is exceeded, the excess rows are
     * silently dropped. The value must be >= 0, and 0 means there is not limit.
     */
    private int maxRows = 0;

    private boolean retrieveSchema = true;
    private List<String> colNames;
    private List<TTypeId> colTypes;
    private List<JdbcColumnAttributes> colAttributes;
    private int fetchSize = 50;
    private boolean emptyResultSet = false;
    private boolean isScrollable = false;
    private ReentrantLock transportLock = null;

    public Builder(Statement statement) throws SQLException {
      this.statement = statement;
      this.connection = statement.getConnection();
    }

    public Builder(Connection connection) {
      this.statement = null;
      this.connection = connection;
    }

    public Builder setClient(TCLIService.Iface client) {
      this.client = client;
      return this;
    }

    public Builder setStmtHandle(TOperationHandle stmtHandle) {
      this.stmtHandle = stmtHandle;
      return this;
    }

    public Builder setSessionHandle(TSessionHandle sessHandle) {
      this.sessHandle = sessHandle;
      return this;
    }

    public Builder setMaxRows(int maxRows) {
      this.maxRows = maxRows;
      return this;
    }

    public Builder setSchema(List<String> colNames, List<TTypeId> colTypes) {
      // no column attributes provided - create list of null attributes.
      List<JdbcColumnAttributes> colAttributes = new ArrayList<>();
      for (int idx = 0; idx < colTypes.size(); ++idx) {
        colAttributes.add(null);
      }
      return setSchema(colNames, colTypes, colAttributes);
    }

    public Builder setSchema(
        List<String> colNames, List<TTypeId> colTypes, List<JdbcColumnAttributes> colAttributes) {
      this.colNames = new ArrayList<>();
      this.colNames.addAll(colNames);
      this.colTypes = new ArrayList<>();
      this.colTypes.addAll(colTypes);
      this.colAttributes = new ArrayList<>();
      this.colAttributes.addAll(colAttributes);
      this.retrieveSchema = false;
      return this;
    }

    public Builder setFetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
      return this;
    }

    public Builder setEmptyResultSet(boolean emptyResultSet) {
      this.emptyResultSet = emptyResultSet;
      return this;
    }

    public Builder setScrollable(boolean setScrollable) {
      this.isScrollable = setScrollable;
      return this;
    }

    public Builder setTransportLock(ReentrantLock transportLock) {
      this.transportLock = transportLock;
      return this;
    }

    public KyuubiArrowQueryResultSet build() throws SQLException {
      return new KyuubiArrowQueryResultSet(this);
    }

    public TProtocolVersion getProtocolVersion() throws SQLException {
      return ((KyuubiConnection) connection).getProtocol();
    }
  }

  protected KyuubiArrowQueryResultSet(Builder builder) throws SQLException {
    this.statement = builder.statement;
    this.client = builder.client;
    this.stmtHandle = builder.stmtHandle;
    this.sessHandle = builder.sessHandle;
    this.fetchSize = builder.fetchSize;
    columnNames = new ArrayList<>();
    normalizedColumnNames = new ArrayList<>();
    columnTypes = new ArrayList<>();
    columnAttributes = new ArrayList<>();
    if (builder.retrieveSchema) {
      retrieveSchema();
    } else {
      this.setSchema(builder.colNames, builder.colTypes, builder.colAttributes);
    }
    this.emptyResultSet = builder.emptyResultSet;
    if (builder.emptyResultSet) {
      this.maxRows = 0;
    } else {
      this.maxRows = builder.maxRows;
    }
    this.isScrollable = builder.isScrollable;
    this.protocol = builder.getProtocolVersion();
    arrowSchema =
        ArrowUtils.toArrowSchema(
            columnNames, convertComplexTypeToStringType(columnTypes), columnAttributes);
    if (allocator == null) {
      initArrowSchemaAndAllocator();
    }
  }

  /**
   * Generate ColumnAttributes object from a TTypeQualifiers
   *
   * @param primitiveTypeEntry primitive type
   * @return generated ColumnAttributes, or null
   */
  public static JdbcColumnAttributes getColumnAttributes(TPrimitiveTypeEntry primitiveTypeEntry) {
    JdbcColumnAttributes ret = null;
    if (primitiveTypeEntry.isSetTypeQualifiers()) {
      TTypeQualifiers tq = primitiveTypeEntry.getTypeQualifiers();
      switch (primitiveTypeEntry.getType()) {
        case CHAR_TYPE:
        case VARCHAR_TYPE:
          TTypeQualifierValue val =
              tq.getQualifiers().get(TCLIServiceConstants.CHARACTER_MAXIMUM_LENGTH);
          if (val != null) {
            // precision is char length
            ret = new JdbcColumnAttributes(val.getI32Value(), 0);
          }
          break;
        case DECIMAL_TYPE:
          TTypeQualifierValue prec = tq.getQualifiers().get(TCLIServiceConstants.PRECISION);
          TTypeQualifierValue scale = tq.getQualifiers().get(TCLIServiceConstants.SCALE);
          ret =
              new JdbcColumnAttributes(
                  prec == null ? HiveDecimal.USER_DEFAULT_PRECISION : prec.getI32Value(),
                  scale == null ? HiveDecimal.USER_DEFAULT_SCALE : scale.getI32Value());
          break;
        case TIMESTAMP_TYPE:
          TTypeQualifierValue timeZone = tq.getQualifiers().get("session.timeZone");
          ret = new JdbcColumnAttributes(timeZone == null ? "" : timeZone.getStringValue());
          break;
        default:
          break;
      }
    }
    return ret;
  }

  /** Retrieve schema from the server */
  private void retrieveSchema() throws SQLException {
    try {
      TGetResultSetMetadataReq metadataReq = new TGetResultSetMetadataReq(stmtHandle);
      // TODO need session handle
      TGetResultSetMetadataResp metadataResp;
      metadataResp = client.GetResultSetMetadata(metadataReq);
      Utils.verifySuccess(metadataResp.getStatus());

      StringBuilder namesSb = new StringBuilder();
      StringBuilder typesSb = new StringBuilder();

      TTableSchema schema = metadataResp.getSchema();
      if (schema == null || !schema.isSetColumns()) {
        // TODO: should probably throw an exception here.
        return;
      }
      setSchema(schema);

      List<TColumnDesc> columns = schema.getColumns();
      for (int pos = 0; pos < schema.getColumnsSize(); pos++) {
        if (pos != 0) {
          namesSb.append(",");
          typesSb.append(",");
        }
        String columnName = columns.get(pos).getColumnName();
        columnNames.add(columnName);
        normalizedColumnNames.add(columnName.toLowerCase());
        TPrimitiveTypeEntry primitiveTypeEntry =
            columns.get(pos).getTypeDesc().getTypes().get(0).getPrimitiveEntry();
        columnTypes.add(primitiveTypeEntry.getType());
        columnAttributes.add(getColumnAttributes(primitiveTypeEntry));
      }
      arrowSchema =
          ArrowUtils.toArrowSchema(
              columnNames, convertComplexTypeToStringType(columnTypes), columnAttributes);
    } catch (SQLException eS) {
      throw eS; // rethrow the SQLException as is
    } catch (Exception ex) {
      ex.printStackTrace();
      throw new KyuubiSQLException("Could not create ResultSet: " + ex.getMessage(), ex);
    }
  }

  /** Set the specified schema to the resultset */
  private void setSchema(
      List<String> colNames, List<TTypeId> colTypes, List<JdbcColumnAttributes> colAttributes) {
    columnNames.addAll(colNames);
    columnTypes.addAll(colTypes);
    columnAttributes.addAll(colAttributes);

    for (String colName : colNames) {
      normalizedColumnNames.add(colName.toLowerCase());
    }
  }

  @Override
  public void close() throws SQLException {
    super.close();
    if (this.statement != null && (this.statement instanceof KyuubiStatement)) {
      KyuubiStatement s = (KyuubiStatement) this.statement;
      s.closeClientOperation();
    } else {
      // for those stmtHandle passed from HiveDatabaseMetaData instead of Statement
      closeOperationHandle(stmtHandle);
    }

    // Need reset during re-open when needed
    client = null;
    stmtHandle = null;
    sessHandle = null;
    isClosed = true;
  }

  private void closeOperationHandle(TOperationHandle stmtHandle) throws SQLException {
    try {
      if (stmtHandle != null) {
        TCloseOperationReq closeReq = new TCloseOperationReq(stmtHandle);
        TCloseOperationResp closeResp = client.CloseOperation(closeReq);
        Utils.verifySuccessWithInfo(closeResp.getStatus());
      }
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new KyuubiSQLException(e.toString(), "08S01", e);
    }
  }

  /**
   * Moves the cursor down one row from its current position.
   *
   * @throws SQLException if a database access error occurs.
   * @see java.sql.ResultSet#next()
   */
  @Override
  public boolean next() throws SQLException {
    if (isClosed) {
      throw new KyuubiSQLException("Resultset is closed");
    }
    if (emptyResultSet || (maxRows > 0 && rowsFetched >= maxRows)) {
      return false;
    }

    /*
     * Poll on the operation status, till the operation is complete. We need to wait only for
     * HiveStatement to complete. HiveDatabaseMetaData which also uses this ResultSet returns only
     * after the RPC is complete.
     */
    if ((statement != null) && (statement instanceof KyuubiStatement)) {
      ((KyuubiStatement) statement).waitForOperationToComplete();
    }

    try {
      TFetchOrientation orientation = TFetchOrientation.FETCH_NEXT;
      if (fetchFirst) {
        // If we are asked to start from begining, clear the current fetched resultset
        orientation = TFetchOrientation.FETCH_FIRST;
        fetchedRowsItr = null;
        fetchFirst = false;
      }

      if (fetchedRowsItr == null || !fetchedRowsItr.hasNext()) {

        TFetchResultsReq fetchReq = new TFetchResultsReq(stmtHandle, orientation, fetchSize);
        TFetchResultsResp fetchResp;
        fetchResp = client.FetchResults(fetchReq);
        Utils.verifySuccessWithInfo(fetchResp.getStatus());

        TRowSet results = fetchResp.getResults();
        if (results == null || results.getColumnsSize() == 0) {
          return false;
        }
        TColumn arrowColumn = results.getColumns().get(0);
        byte[] batchBytes = arrowColumn.getBinaryVal().getValues().get(0).array();
        ArrowRecordBatch recordBatch = loadArrowBatch(batchBytes, allocator);
        VectorLoader vectorLoader = new VectorLoader(root);
        vectorLoader.load(recordBatch);
        recordBatch.close();
        java.util.List<ArrowColumnVector> columns =
            root.getFieldVectors().stream()
                .map(vector -> new ArrowColumnVector(vector))
                .collect(Collectors.toList());
        ArrowColumnarBatch batch =
            new ArrowColumnarBatch(columns.toArray(new ArrowColumnVector[0]), root.getRowCount());
        fetchedRowsItr = batch.rowIterator();
      }

      if (fetchedRowsItr.hasNext()) {
        row = fetchedRowsItr.next();
      } else {
        return false;
      }

      rowsFetched++;
    } catch (SQLException eS) {
      throw eS;
    } catch (Exception ex) {
      ex.printStackTrace();
      throw new KyuubiSQLException("Error retrieving next row", ex);
    }
    // NOTE: fetchOne doesn't throw new SQLFeatureNotSupportedException("Method not supported").
    return true;
  }

  private ArrowRecordBatch loadArrowBatch(byte[] batchBytes, BufferAllocator allocator)
      throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(batchBytes);
    return MessageSerializer.deserializeRecordBatch(
        new ReadChannel(Channels.newChannel(in)), allocator);
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    if (isClosed) {
      throw new KyuubiSQLException("Resultset is closed");
    }
    return super.getMetaData();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    if (isClosed) {
      throw new KyuubiSQLException("Resultset is closed");
    }
    fetchSize = rows;
  }

  @Override
  public int getType() throws SQLException {
    if (isClosed) {
      throw new KyuubiSQLException("Resultset is closed");
    }
    if (isScrollable) {
      return ResultSet.TYPE_SCROLL_INSENSITIVE;
    } else {
      return ResultSet.TYPE_FORWARD_ONLY;
    }
  }

  @Override
  public int getFetchSize() throws SQLException {
    if (isClosed) {
      throw new KyuubiSQLException("Resultset is closed");
    }
    return fetchSize;
  }

  /**
   * Moves the cursor before the first row of the resultset.
   *
   * @throws SQLException if a database access error occurs.
   * @see java.sql.ResultSet#next()
   */
  @Override
  public void beforeFirst() throws SQLException {
    if (isClosed) {
      throw new KyuubiSQLException("Resultset is closed");
    }
    if (!isScrollable) {
      throw new KyuubiSQLException("Method not supported for TYPE_FORWARD_ONLY resultset");
    }
    fetchFirst = true;
    rowsFetched = 0;
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    if (isClosed) {
      throw new KyuubiSQLException("Resultset is closed");
    }
    return (rowsFetched == 0);
  }

  @Override
  public int getRow() throws SQLException {
    return rowsFetched;
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  private List<TTypeId> convertComplexTypeToStringType(List<TTypeId> colTypes) {
    if (convertComplexTypeToString) {
      return colTypes.stream()
          .map(
              type -> {
                if (type == TTypeId.ARRAY_TYPE
                    || type == TTypeId.MAP_TYPE
                    || type == TTypeId.STRUCT_TYPE) {
                  return TTypeId.STRING_TYPE;
                } else {
                  return type;
                }
              })
          .collect(Collectors.toList());
    } else {
      return colTypes;
    }
  }
}
