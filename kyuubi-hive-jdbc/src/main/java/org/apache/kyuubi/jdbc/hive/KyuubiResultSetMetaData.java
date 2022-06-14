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

/** KyuubiResultSetMetaData. */
public class KyuubiResultSetMetaData implements SQLResultSetMetaData {
  private final List<String> columnNames;
  private final List<String> columnTypes;
  private final List<JdbcColumnAttributes> columnAttributes;

  public KyuubiResultSetMetaData(
      List<String> columnNames,
      List<String> columnTypes,
      List<JdbcColumnAttributes> columnAttributes) {
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.columnAttributes = columnAttributes;
  }

  private int getSqlType(int column) throws SQLException {
    return JdbcColumn.typeStringToSqlType(columnTypes.get(toZeroIndex(column)));
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
    String type = columnTypes.get(toZeroIndex(column));

    // we need to convert the thrift type to the SQL type
    return JdbcColumn.typeStringToSqlType(type);
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
    // we need to convert the Hive type to the SQL type name
    // TODO: this would be better handled in an enum
    String type = columnTypes.get(toZeroIndex(column));
    return "string".equalsIgnoreCase(type);
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
      throw new SQLException("Could not determine column type name for ResultSet");
    }
    if (column < 1 || column > columnTypes.size()) {
      throw new SQLException("Invalid column value: " + column);
    }
    return column - 1;
  }
}
