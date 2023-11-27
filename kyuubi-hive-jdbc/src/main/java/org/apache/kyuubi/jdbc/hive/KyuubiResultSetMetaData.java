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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import org.apache.kyuubi.jdbc.hive.adapter.SQLResultSetMetaData;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId;

/** KyuubiResultSetMetaData. */
public class KyuubiResultSetMetaData implements SQLResultSetMetaData {
  private final List<String> columnNames;
  private final List<TTypeId> columnTypes;
  private final List<JdbcColumnAttributes> columnAttributes;

  public KyuubiResultSetMetaData(
      List<String> columnNames,
      List<TTypeId> columnTypes,
      List<JdbcColumnAttributes> columnAttributes) {
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.columnAttributes = columnAttributes;
  }

  private int getSqlType(int column) throws SQLException {
    return JdbcColumn.convertTTypeIdToSqlType(columnTypes.get(toZeroIndex(column)));
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    return JdbcColumn.columnClassName(
        columnTypes.get(toZeroIndex(column)), columnAttributes.get(toZeroIndex(column)));
  }

  @Override
  public int getColumnCount() throws SQLException {
    return columnNames.size();
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return JdbcColumn.columnDisplaySize(
        columnTypes.get(toZeroIndex(column)), columnAttributes.get(toZeroIndex(column)));
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return columnNames.get(toZeroIndex(column));
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return columnNames.get(toZeroIndex(column));
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    // we need to convert the thrift type to the SQL type
    TTypeId type = columnTypes.get(toZeroIndex(column));

    // we need to convert the thrift type to the SQL type
    return JdbcColumn.convertTTypeIdToSqlType(type);
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return JdbcColumn.getColumnTypeName(columnTypes.get(toZeroIndex(column)));
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return JdbcColumn.columnPrecision(
        columnTypes.get(toZeroIndex(column)), columnAttributes.get(toZeroIndex(column)));
  }

  @Override
  public int getScale(int column) throws SQLException {
    return JdbcColumn.columnScale(getSqlType(column), columnAttributes.get(toZeroIndex(column)));
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    // Hive doesn't have an auto-increment concept
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    TTypeId type = columnTypes.get(toZeroIndex(column));
    return type == TTypeId.STRING_TYPE;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    // Hive doesn't support a currency type
    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    // Hive doesn't have the concept of not-null
    return ResultSetMetaData.columnNullable;
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return true;
  }

  protected int toZeroIndex(int column) throws SQLException {
    if (columnTypes == null) {
      throw new KyuubiSQLException("Could not determine column type name for ResultSet");
    }
    if (column < 1 || column > columnTypes.size()) {
      throw new KyuubiSQLException("Invalid column value: " + column);
    }
    return column - 1;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    TTypeId typeId = columnTypes.get(toZeroIndex(column));
    switch (typeId) {
      case TINYINT_TYPE:
      case SMALLINT_TYPE:
      case INT_TYPE:
      case BIGINT_TYPE:
      case FLOAT_TYPE:
      case DOUBLE_TYPE:
      case DECIMAL_TYPE:
      case TIMESTAMP_TYPE:
      case DATE_TYPE:
      case INTERVAL_YEAR_MONTH_TYPE:
      case INTERVAL_DAY_TIME_TYPE:
      case TIMESTAMPLOCALTZ_TYPE:
        return true;

      case BOOLEAN_TYPE:
      case STRING_TYPE:
      case VARCHAR_TYPE:
      case CHAR_TYPE:
      case NULL_TYPE:
      case BINARY_TYPE:
      case ARRAY_TYPE:
      case MAP_TYPE:
      case STRUCT_TYPE:
      case UNION_TYPE:
      case USER_DEFINED_TYPE:
      default:
        return false;
    }
  }
}
