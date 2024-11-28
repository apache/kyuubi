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
import java.util.Calendar;
import java.util.List;
import org.apache.kyuubi.jdbc.hive.adapter.SQLResultSet;
import org.apache.kyuubi.jdbc.hive.common.HiveIntervalDayTime;
import org.apache.kyuubi.jdbc.hive.common.HiveIntervalYearMonth;
import org.apache.kyuubi.jdbc.hive.common.TimestampTZUtil;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTableSchema;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId;

/** Data independent base class which implements the common part of all Kyuubi result sets. */
public abstract class KyuubiBaseResultSet implements SQLResultSet {

  protected Statement statement = null;
  protected SQLWarning warningChain = null;
  protected boolean wasNull = false;
  protected Object[] row;
  protected List<String> columnNames;
  protected List<String> normalizedColumnNames;
  protected List<TTypeId> columnTypes;
  protected List<JdbcColumnAttributes> columnAttributes;

  private TTableSchema schema;

  @Override
  public int findColumn(String columnName) throws SQLException {
    int columnIndex = 0;
    if (columnName != null) {
      final String lcColumnName = columnName.toLowerCase();
      for (final String normalizedColumnName : normalizedColumnNames) {
        ++columnIndex;
        final int idx = normalizedColumnName.lastIndexOf('.');
        final String name =
            (idx == -1) ? normalizedColumnName : normalizedColumnName.substring(1 + idx);
        if (name.equals(lcColumnName) || normalizedColumnName.equals(lcColumnName)) {
          return columnIndex;
        }
      }
    }
    throw new KyuubiSQLException("Could not find " + columnName + " in " + normalizedColumnNames);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    final Object val = getObject(columnIndex);
    if (val instanceof BigDecimal) {
      return (BigDecimal) val;
    }
    throw new KyuubiSQLException(
        "Illegal conversion to BigDecimal from column " + columnIndex + " [" + val + "]");
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
    final Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    }
    if (obj instanceof InputStream) {
      return (InputStream) obj;
    }
    if (obj instanceof byte[]) {
      byte[] byteArray = (byte[]) obj;
      return new ByteArrayInputStream(byteArray);
    }
    if (obj instanceof String) {
      final String str = (String) obj;
      return new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8));
    }
    throw new KyuubiSQLException(
        "Illegal conversion to binary stream from column " + columnIndex + " [" + obj + "]");
  }

  @Override
  public InputStream getBinaryStream(String columnName) throws SQLException {
    return getBinaryStream(findColumn(columnName));
  }

  /**
   * Retrieves the value of the designated column in the current row of this ResultSet object as a
   * boolean in the Java programming language. If the designated column has a datatype of CHAR or
   * VARCHAR and contains a "0" or has a datatype of BIT, TINYINT, SMALLINT, INTEGER or BIGINT and
   * contains a 0, a value of false is returned.
   *
   * <p>For a true value, this implementation relaxes the JDBC specs. If the designated column has a
   * datatype of CHAR or VARCHAR and contains a values other than "0" or has a datatype of BIT,
   * TINYINT, SMALLINT, INTEGER or BIGINT and contains a value other than 0, a value of true is
   * returned.
   *
   * @param columnIndex the first column is 1, the second is 2, ...
   * @return the column value; if the value is SQL NULL, the value returned is false
   * @throws if the columnIndex is not valid; if a database access error occurs or this method is
   *     called on a closed result set
   * @see ResultSet#getBoolean(int)
   */
  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    final Object obj = getObject(columnIndex);
    if (obj == null) {
      return false;
    }
    if (obj instanceof Boolean) {
      return (Boolean) obj;
    }
    if (obj instanceof Number) {
      return ((Number) obj).intValue() != 0;
    }
    if (obj instanceof String) {
      return !"0".equals(obj);
    }
    throw new KyuubiSQLException(
        "Illegal conversion to boolean from column " + columnIndex + " [" + obj + "]");
  }

  /**
   * Retrieves the value of the designated column in the current row of this ResultSet object as a
   * boolean in the Java programming language. If the designated column has a datatype of CHAR or
   * VARCHAR and contains a "0" or has a datatype of BIT, TINYINT, SMALLINT, INTEGER or BIGINT and
   * contains a 0, a value of false is returned.
   *
   * <p>For a true value, this implementation relaxes the JDBC specs. If the designated column has a
   * datatype of CHAR or VARCHAR and contains a values other than "0" or has a datatype of BIT,
   * TINYINT, SMALLINT, INTEGER or BIGINT and contains a value other than 0, a value of true is
   * returned.
   *
   * @param columnLabel the label for the column specified with the SQL AS clause. If the SQL AS
   *     clause was not specified, then the label is the name of the column
   * @return the column value; if the value is SQL NULL, the value returned is false
   * @throws if the columnIndex is not valid; if a database access error occurs or this method is
   *     called on a closed result set
   * @see ResultSet#getBoolean(String)
   */
  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return getBoolean(findColumn(columnLabel));
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    final Object obj = getObject(columnIndex);
    if (obj == null) {
      return 0;
    }
    if (obj instanceof Number) {
      return ((Number) obj).byteValue();
    }
    throw new KyuubiSQLException(
        "Illegal conversion to byte from column " + columnIndex + " [" + obj + "]");
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
    final Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    }
    if (obj instanceof Date) {
      return (Date) obj;
    }
    if (obj instanceof String) {
      try {
        return Date.valueOf((String) obj);
      } catch (Exception e) {
        throw new SQLException(
            "Illegal conversion to Date from column " + columnIndex + " [" + obj + "]", e);
      }
    }
    throw new SQLException(
        "Illegal conversion to Date from column " + columnIndex + " [" + obj + "]");
  }

  @Override
  public Date getDate(String columnName) throws SQLException {
    return getDate(findColumn(columnName));
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    Date value = getDate(columnIndex);
    if (value == null) {
      return null;
    }
    try {
      return parseDate(value, cal);
    } catch (IllegalArgumentException e) {
      throw new KyuubiSQLException("Cannot convert column " + columnIndex + " to date: " + e, e);
    }
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return this.getDate(findColumn(columnLabel), cal);
  }

  private Date parseDate(Date value, Calendar cal) {
    if (cal == null) {
      cal = Calendar.getInstance();
    }
    cal.setTime(value);
    return new Date(cal.getTimeInMillis());
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    final Object obj = getObject(columnIndex);
    if (obj == null) {
      return 0.0;
    }
    if (obj instanceof Number) {
      return ((Number) obj).doubleValue();
    }
    if (obj instanceof String) {
      try {
        return Double.parseDouble((String) obj);
      } catch (Exception e) {
        throw new KyuubiSQLException(
            "Illegal conversion to double from column " + columnIndex + " [" + obj + "]", e);
      }
    }
    throw new KyuubiSQLException(
        "Illegal conversion to double from column " + columnIndex + " [" + obj + "]");
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
    final Object obj = getObject(columnIndex);
    if (obj == null) {
      return 0.0f;
    }
    if (obj instanceof Number) {
      return ((Number) obj).floatValue();
    }
    if (obj instanceof String) {
      try {
        return Float.parseFloat((String) obj);
      } catch (Exception e) {
        throw new KyuubiSQLException(
            "Illegal conversion to float from column " + columnIndex + " [" + obj + "]", e);
      }
    }
    throw new KyuubiSQLException(
        "Illegal conversion to float from column " + columnIndex + " [" + obj + "]");
  }

  @Override
  public float getFloat(String columnName) throws SQLException {
    return getFloat(findColumn(columnName));
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    final Object obj = getObject(columnIndex);
    if (obj == null) {
      return 0;
    }
    if (obj instanceof Number) {
      return ((Number) obj).intValue();
    }
    if (obj instanceof String) {
      try {
        return Integer.parseInt((String) obj);
      } catch (Exception e) {
        throw new KyuubiSQLException(
            "Illegal conversion to int from column " + columnIndex + " [" + obj + "]", e);
      }
    }
    throw new KyuubiSQLException(
        "Illegal conversion to int from column " + columnIndex + " [" + obj + "]");
  }

  @Override
  public int getInt(String columnName) throws SQLException {
    return getInt(findColumn(columnName));
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    final Object obj = getObject(columnIndex);
    if (obj == null) {
      return 0L;
    }
    if (obj instanceof Number) {
      return ((Number) obj).longValue();
    }
    if (obj instanceof String) {
      try {
        return Long.parseLong((String) obj);
      } catch (Exception e) {
        throw new KyuubiSQLException(
            "Illegal conversion to long from column " + columnIndex + " [" + obj + "]", e);
      }
    }
    throw new KyuubiSQLException(
        "Illegal conversion to long from column " + columnIndex + " [" + obj + "]");
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
      throw new KyuubiSQLException("No row found");
    }
    if (row.length == 0) {
      throw new KyuubiSQLException("RowSet does not contain any columns");
    }
    // the first column is 1, the second is 2, ...
    if (columnIndex <= 0 || columnIndex > row.length) {
      throw new KyuubiSQLException("Invalid columnIndex: " + columnIndex);
    }
    final TTypeId columnType = columnTypes.get(columnIndex - 1);
    final Object value = row[columnIndex - 1];

    try {
      final Object evaluated = evaluate(columnType, value);
      wasNull = evaluated == null;
      return evaluated;
    } catch (Exception e) {
      throw new KyuubiSQLException("Failed to evaluate " + columnType + " [" + value + "]", e);
    }
  }

  private Object evaluate(TTypeId columnType, Object value) {
    if (value == null) {
      return null;
    }
    switch (columnType) {
      case BINARY_TYPE:
        if (value instanceof String) {
          return ((String) value).getBytes(StandardCharsets.UTF_8);
        }
        return value;
      case TIMESTAMP_TYPE:
        return Timestamp.valueOf((String) value);
      case TIMESTAMPLOCALTZ_TYPE:
        return TimestampTZUtil.parse((String) value);
      case DECIMAL_TYPE:
        return new BigDecimal((String) value);
      case DATE_TYPE:
        return Date.valueOf((String) value);
      case INTERVAL_YEAR_MONTH_TYPE:
        return HiveIntervalYearMonth.valueOf((String) value);
      case INTERVAL_DAY_TIME_TYPE:
        return HiveIntervalDayTime.valueOf((String) value);
      case ARRAY_TYPE:
      case MAP_TYPE:
      case STRUCT_TYPE:
        // todo: returns json string. should recreate object from it?
        return value;
      default:
        return value;
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
    final Object obj = getObject(columnIndex);
    if (obj instanceof Number) {
      return ((Number) obj).shortValue();
    } else if (obj == null) {
      return 0;
    } else if (obj instanceof String) {
      try {
        return Short.parseShort((String) obj);
      } catch (Exception e) {
        throw new KyuubiSQLException(
            "Illegal conversion to int from column " + columnIndex + " [" + obj + "]", e);
      }
    } else {
      throw new KyuubiSQLException(
          "Illegal conversion to int from column " + columnIndex + " [" + obj + "]");
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
    final Object value = getColumnValue(columnIndex);
    if (value == null) {
      return null;
    }
    if (value instanceof byte[]) {
      return new String((byte[]) value, StandardCharsets.UTF_8);
    }
    return value.toString();
  }

  @Override
  public String getString(String columnName) throws SQLException {
    return getString(findColumn(columnName));
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    final Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    }
    if (obj instanceof Timestamp) {
      return (Timestamp) obj;
    }
    if (obj instanceof String) {
      try {
        return Timestamp.valueOf((String) obj);
      } catch (Exception e) {
        throw new KyuubiSQLException(
            "Illegal conversion to int from column " + columnIndex + " [" + obj + "]", e);
      }
    }
    throw new KyuubiSQLException(
        "Illegal conversion to int from column " + columnIndex + " [" + obj + "]");
  }

  @Override
  public Timestamp getTimestamp(String columnName) throws SQLException {
    return getTimestamp(findColumn(columnName));
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    Timestamp value = getTimestamp(columnIndex);
    if (value == null) {
      return null;
    }
    try {
      return parseTimestamp(value, cal);
    } catch (IllegalArgumentException e) {
      throw new KyuubiSQLException(
          "Cannot convert column " + columnIndex + " to timestamp: " + e, e);
    }
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return this.getTimestamp(findColumn(columnLabel), cal);
  }

  private Timestamp parseTimestamp(Timestamp timestamp, Calendar cal) {
    if (cal == null) {
      cal = Calendar.getInstance();
    }
    long v = timestamp.getTime();
    cal.setTimeInMillis(v);
    timestamp = new Timestamp(cal.getTime().getTime());
    return timestamp;
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    }
    if (obj instanceof Time) {
      return (Time) obj;
    }
    if (obj instanceof String) {
      return Time.valueOf((String) obj);
    }
    throw new KyuubiSQLException("Illegal conversion");
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return getTime(findColumn(columnLabel));
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    Time value = getTime(columnIndex);
    if (value == null) {
      return null;
    }
    try {
      return parseTime(value, cal);
    } catch (IllegalArgumentException e) {
      throw new KyuubiSQLException("Cannot convert column " + columnIndex + " to time: " + e, e);
    }
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return this.getTime(findColumn(columnLabel), cal);
  }

  private Time parseTime(Time date, Calendar cal) {
    if (cal == null) {
      cal = Calendar.getInstance();
    }
    long v = date.getTime();
    cal.setTimeInMillis(v);
    date = new Time(cal.getTime().getTime());
    return date;
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
}
