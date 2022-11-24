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
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hive.service.rpc.thrift.TTableSchema;
import org.apache.hive.service.rpc.thrift.TTypeId;
import org.apache.kyuubi.jdbc.hive.adapter.SQLResultSet;
import org.apache.kyuubi.jdbc.hive.arrow.ArrowColumnarBatchRow;
import org.apache.kyuubi.jdbc.hive.arrow.ArrowUtils;

/** Data independent base class which implements the common part of all Kyuubi result sets. */
public abstract class KyuubiArrowBasedResultSet implements SQLResultSet {

  protected Statement statement = null;
  protected SQLWarning warningChain = null;
  protected boolean wasNull = false;
  protected List<String> columnNames;
  protected List<String> normalizedColumnNames;
  protected List<TTypeId> columnTypes;
  protected List<JdbcColumnAttributes> columnAttributes;
  private TTableSchema schema;

  // arrow related
  protected Schema arrowSchema;
  protected VectorSchemaRoot root;
  protected ArrowColumnarBatchRow row;

  protected BufferAllocator allocator;

  protected void initArrowSchemaAndAllocator() {
    if (arrowSchema == null) {
      throw new IllegalStateException("arrow schema is null");
    }
    allocator = ArrowUtils.rootAllocator.newChildAllocator("ArrowResultSet", 0, Long.MAX_VALUE);
    root = VectorSchemaRoot.create(arrowSchema, allocator);
  }

  @Override
  public int findColumn(String columnName) throws SQLException {
    int columnIndex = 0;
    boolean findColumn = false;
    for (String normalizedColumnName : normalizedColumnNames) {
      ++columnIndex;
      String[] names = normalizedColumnName.split("\\.");
      String name = names[names.length - 1];
      if (name.equalsIgnoreCase(columnName) || normalizedColumnName.equalsIgnoreCase(columnName)) {
        findColumn = true;
        break;
      }
    }
    if (!findColumn) {
      throw new KyuubiSQLException("Could not find " + columnName + " in " + normalizedColumnNames);
    } else {
      return columnIndex;
    }
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    Object val = getObject(columnIndex);

    if (val == null || val instanceof BigDecimal) {
      return (BigDecimal) val;
    }

    throw new KyuubiSQLException("Illegal conversion");
  }

  @Override
  public BigDecimal getBigDecimal(String columnName) throws SQLException {
    return getBigDecimal(findColumn(columnName));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    MathContext mc = new MathContext(scale);
    return getBigDecimal(columnIndex).round(mc);
  }

  @Override
  public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
    return getBigDecimal(findColumn(columnName), scale);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    } else if (obj instanceof InputStream) {
      return (InputStream) obj;
    } else if (obj instanceof byte[]) {
      byte[] byteArray = (byte[]) obj;
      return new ByteArrayInputStream(byteArray);
    } else if (obj instanceof String) {
      String str = (String) obj;
      return new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8));
    }
    throw new KyuubiSQLException("Illegal conversion to binary stream from column " + columnIndex);
  }

  @Override
  public InputStream getBinaryStream(String columnName) throws SQLException {
    return getBinaryStream(findColumn(columnName));
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (obj instanceof Boolean) {
      return (Boolean) obj;
    } else if (obj == null) {
      return false;
    } else if (obj instanceof Number) {
      return ((Number) obj).intValue() != 0;
    } else if (obj instanceof String) {
      return !obj.equals("0");
    }
    throw new KyuubiSQLException("Cannot convert column " + columnIndex + " to boolean");
  }

  @Override
  public boolean getBoolean(String columnName) throws SQLException {
    return getBoolean(findColumn(columnName));
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (obj instanceof Number) {
      return ((Number) obj).byteValue();
    } else if (obj == null) {
      return 0;
    }
    throw new KyuubiSQLException("Cannot convert column " + columnIndex + " to byte");
  }

  @Override
  public byte getByte(String columnName) throws SQLException {
    return getByte(findColumn(columnName));
  }

  @Override
  public int getConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    }
    if (obj instanceof Date) {
      return (Date) obj;
    }
    try {
      if (obj instanceof String) {
        return Date.valueOf((String) obj);
      }
    } catch (Exception e) {
      throw new KyuubiSQLException("Cannot convert column " + columnIndex + " to date: " + e, e);
    }
    // If we fell through to here this is not a valid type conversion
    throw new KyuubiSQLException(
        "Cannot convert column " + columnIndex + " to date: Illegal conversion");
  }

  @Override
  public Date getDate(String columnName) throws SQLException {
    return getDate(findColumn(columnName));
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (obj instanceof Number) {
        return ((Number) obj).doubleValue();
      } else if (obj == null) {
        return 0;
      } else if (obj instanceof String) {
        return Double.parseDouble((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new KyuubiSQLException("Cannot convert column " + columnIndex + " to double: " + e, e);
    }
  }

  @Override
  public double getDouble(String columnName) throws SQLException {
    return getDouble(findColumn(columnName));
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (obj instanceof Number) {
        return ((Number) obj).floatValue();
      } else if (obj == null) {
        return 0;
      } else if (obj instanceof String) {
        return Float.parseFloat((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new KyuubiSQLException("Cannot convert column " + columnIndex + " to float: " + e, e);
    }
  }

  @Override
  public float getFloat(String columnName) throws SQLException {
    return getFloat(findColumn(columnName));
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (obj instanceof Number) {
        return ((Number) obj).intValue();
      } else if (obj == null) {
        return 0;
      } else if (obj instanceof String) {
        return Integer.parseInt((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new KyuubiSQLException("Cannot convert column " + columnIndex + " to integer" + e, e);
    }
  }

  @Override
  public int getInt(String columnName) throws SQLException {
    return getInt(findColumn(columnName));
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (obj instanceof Number) {
        return ((Number) obj).longValue();
      } else if (obj == null) {
        return 0;
      } else if (obj instanceof String) {
        return Long.parseLong((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new KyuubiSQLException("Cannot convert column " + columnIndex + " to long: " + e, e);
    }
  }

  @Override
  public long getLong(String columnName) throws SQLException {
    return getLong(findColumn(columnName));
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return new KyuubiResultSetMetaData(columnNames, columnTypes, columnAttributes);
  }

  private Object getColumnValue(int columnIndex) throws SQLException {
    if (row == null) {
      throw new KyuubiSQLException("No row found.");
    }
    if (row.numFields() == 0) {
      throw new KyuubiSQLException("RowSet does not contain any columns!");
    }
    if (columnIndex > columnNames.size()) {
      throw new KyuubiSQLException("Invalid columnIndex: " + columnIndex);
    }
    TTypeId columnType = columnTypes.get(columnIndex - 1);

    try {
      wasNull = row.isNullAt(columnIndex - 1);
      if (wasNull) {
        return null;
      } else {
        return row.get(columnIndex - 1, columnType);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new KyuubiSQLException("Unrecognized column type:", e);
    }
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return getColumnValue(columnIndex);
  }

  @Override
  public Object getObject(String columnName) throws SQLException {
    return getObject(findColumn(columnName));
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (obj instanceof Number) {
        return ((Number) obj).shortValue();
      } else if (obj == null) {
        return 0;
      } else if (obj instanceof String) {
        return Short.parseShort((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new KyuubiSQLException("Cannot convert column " + columnIndex + " to short: " + e, e);
    }
  }

  @Override
  public short getShort(String columnName) throws SQLException {
    return getShort(findColumn(columnName));
  }

  @Override
  public Statement getStatement() throws SQLException {
    return this.statement;
  }

  /**
   * @param columnIndex - the first column is 1, the second is 2, ...
   * @see java.sql.ResultSet#getString(int)
   */
  @Override
  public String getString(int columnIndex) throws SQLException {
    Object value = getColumnValue(columnIndex);
    if (wasNull) {
      return null;
    }
    if (value instanceof byte[]) {
      return new String((byte[]) value);
    }
    return value.toString();
  }

  @Override
  public String getString(String columnName) throws SQLException {
    return getString(findColumn(columnName));
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    }
    if (obj instanceof Timestamp) {
      return (Timestamp) obj;
    }
    if (obj instanceof String) {
      return Timestamp.valueOf((String) obj);
    }
    throw new KyuubiSQLException("Illegal conversion");
  }

  @Override
  public Timestamp getTimestamp(String columnName) throws SQLException {
    return getTimestamp(findColumn(columnName));
  }

  @Override
  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    return false;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    return false;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  @Override
  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  @Override
  public boolean wasNull() throws SQLException {
    return wasNull;
  }

  protected void setSchema(TTableSchema schema) {
    this.schema = schema;
  }

  protected TTableSchema getSchema() {
    return schema;
  }

  @Override
  public void close() throws SQLException {
    if (root != null) {
      root.close();
    }
    if (allocator != null) {
      allocator.close();
    }
  }
}
