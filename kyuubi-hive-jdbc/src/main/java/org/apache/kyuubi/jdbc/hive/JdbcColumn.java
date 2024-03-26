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

import static java.sql.Types.*;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.apache.kyuubi.jdbc.hive.common.HiveIntervalDayTime;
import org.apache.kyuubi.jdbc.hive.common.HiveIntervalYearMonth;
import org.apache.kyuubi.jdbc.hive.common.TimestampTZ;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId;

/** Column metadata. */
public class JdbcColumn {
  private final String columnName;
  private final String tableName;
  private final String tableCatalog;
  private final String type;
  private final String comment;
  private final int ordinalPos;

  JdbcColumn(
      String columnName,
      String tableName,
      String tableCatalog,
      String type,
      String comment,
      int ordinalPos) {
    this.columnName = columnName;
    this.tableName = tableName;
    this.tableCatalog = tableCatalog;
    this.type = type;
    this.comment = comment;
    this.ordinalPos = ordinalPos;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTableCatalog() {
    return tableCatalog;
  }

  public String getType() {
    return type;
  }

  public String getComment() {
    return comment;
  }

  public int getOrdinalPos() {
    return ordinalPos;
  }

  public Integer getNumPrecRadix() {
    if (type.equalsIgnoreCase("tinyint")) {
      return 10;
    } else if (type.equalsIgnoreCase("smallint")) {
      return 10;
    } else if (type.equalsIgnoreCase("int")) {
      return 10;
    } else if (type.equalsIgnoreCase("bigint")) {
      return 10;
    } else if (type.equalsIgnoreCase("float")) {
      return 10;
    } else if (type.equalsIgnoreCase("double")) {
      return 10;
    } else if (type.equalsIgnoreCase("decimal")) {
      return 10;
    } else { // anything else including boolean and string is null
      return null;
    }
  }

  static String columnClassName(TTypeId tType, JdbcColumnAttributes columnAttributes)
      throws SQLException {
    int columnType = convertTTypeIdToSqlType(tType);
    switch (columnType) {
      case NULL:
        return "null";
      case BOOLEAN:
        return Boolean.class.getName();
      case CHAR:
      case VARCHAR:
        return String.class.getName();
      case TINYINT:
        return Byte.class.getName();
      case SMALLINT:
        return Short.class.getName();
      case INTEGER:
        return Integer.class.getName();
      case BIGINT:
        return Long.class.getName();
      case DATE:
        return Date.class.getName();
      case FLOAT:
        return Float.class.getName();
      case DOUBLE:
        return Double.class.getName();
      case TIMESTAMP:
        return Timestamp.class.getName();
      case TIMESTAMP_WITH_TIMEZONE:
        return TimestampTZ.class.getName();
      case DECIMAL:
        return BigInteger.class.getName();
      case BINARY:
        return byte[].class.getName();
      case OTHER:
      case JAVA_OBJECT:
        {
          switch (tType) {
            case INTERVAL_YEAR_MONTH_TYPE:
              return HiveIntervalYearMonth.class.getName();
            case INTERVAL_DAY_TIME_TYPE:
              return HiveIntervalDayTime.class.getName();
            default:
              return String.class.getName();
          }
        }
      case ARRAY:
      case STRUCT:
        return String.class.getName();
      default:
        throw new KyuubiSQLException("Invalid column type: " + columnType);
    }
  }

  public static int convertTTypeIdToSqlType(TTypeId type) throws SQLException {
    switch (type) {
      case STRING_TYPE:
      case VARCHAR_TYPE:
        return VARCHAR;
      case CHAR_TYPE:
        return CHAR;
      case FLOAT_TYPE:
        return FLOAT;
      case DOUBLE_TYPE:
        return DOUBLE;
      case BOOLEAN_TYPE:
        return BOOLEAN;
      case TINYINT_TYPE:
        return TINYINT;
      case SMALLINT_TYPE:
        return SMALLINT;
      case INT_TYPE:
        return INTEGER;
      case BIGINT_TYPE:
        return BIGINT;
      case DATE_TYPE:
        return DATE;
      case TIMESTAMP_TYPE:
        return TIMESTAMP;
      case TIMESTAMPLOCALTZ_TYPE:
      case INTERVAL_YEAR_MONTH_TYPE:
      case INTERVAL_DAY_TIME_TYPE:
      case UNION_TYPE:
      case USER_DEFINED_TYPE:
        return OTHER;
      case DECIMAL_TYPE:
        return DECIMAL;
      case BINARY_TYPE:
        return BINARY;
      case MAP_TYPE:
        return JAVA_OBJECT;
      case ARRAY_TYPE:
        return ARRAY;
      case STRUCT_TYPE:
        return STRUCT;
      case NULL_TYPE:
        return NULL;
      default:
        throw new KyuubiSQLException("Invalid column type: " + type);
    }
  }

  static String getColumnTypeName(TTypeId type) throws SQLException {
    switch (type) {
      case STRING_TYPE:
        return "string";
      case VARCHAR_TYPE:
        return "varchar";
      case CHAR_TYPE:
        return "char";
      case FLOAT_TYPE:
        return "float";
      case DOUBLE_TYPE:
        return "double";
      case BOOLEAN_TYPE:
        return "boolean";
      case TINYINT_TYPE:
        return "tinyint";
      case SMALLINT_TYPE:
        return "smallint";
      case INT_TYPE:
        return "int";
      case BIGINT_TYPE:
        return "bigint";
      case DATE_TYPE:
        return "date";
      case TIMESTAMP_TYPE:
        return "timestamp";
      case TIMESTAMPLOCALTZ_TYPE:
        return "timestamp with local time zone";
      case INTERVAL_YEAR_MONTH_TYPE:
        return "interval_year_month";
      case INTERVAL_DAY_TIME_TYPE:
        return "interval_day_time";
      case DECIMAL_TYPE:
        return "decimal";
      case BINARY_TYPE:
        return "binary";
      case MAP_TYPE:
        return "map";
      case ARRAY_TYPE:
        return "array";
      case STRUCT_TYPE:
        return "struct";
      case NULL_TYPE:
        return "void";
      case UNION_TYPE:
        return "uniontype";
      case USER_DEFINED_TYPE:
        return "user_defined";
      default:
        throw new KyuubiSQLException("Invalid column type: " + type);
    }
  }

  static int columnDisplaySize(TTypeId tType, JdbcColumnAttributes columnAttributes)
      throws SQLException {
    int columnType = convertTTypeIdToSqlType(tType);
    switch (columnType) {
      case NULL:
        return 4; // "NULL"
      case BOOLEAN:
        return columnPrecision(tType, columnAttributes);
      case CHAR:
      case VARCHAR:
        return columnPrecision(tType, columnAttributes);
      case BINARY:
        return Integer.MAX_VALUE; // hive has no max limit for binary
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return columnPrecision(tType, columnAttributes) + 1; // allow +/-
      case DATE:
        return 10;
      case TIMESTAMP:
      case TIMESTAMP_WITH_TIMEZONE:
        return columnPrecision(tType, columnAttributes);

        // see
        // http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Float.MAX_EXPONENT
      case FLOAT:
        return 24; // e.g. -(17#).e-###
        // see
        // http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Double.MAX_EXPONENT
      case DOUBLE:
        return 25; // e.g. -(17#).e-####
      case DECIMAL:
        return columnPrecision(tType, columnAttributes) + 2; // '-' sign and '.'
      case OTHER:
      case JAVA_OBJECT:
        return columnPrecision(tType, columnAttributes);
      case ARRAY:
      case STRUCT:
        return Integer.MAX_VALUE;
      default:
        throw new KyuubiSQLException("Invalid column type: " + columnType);
    }
  }

  static int columnPrecision(TTypeId tType, JdbcColumnAttributes columnAttributes)
      throws SQLException {
    int columnType = convertTTypeIdToSqlType(tType);
    // according to hiveTypeToSqlType possible options are:
    switch (columnType) {
      case NULL:
        return 0;
      case BOOLEAN:
        return 1;
      case CHAR:
      case VARCHAR:
        if (columnAttributes != null) {
          return columnAttributes.precision;
        }
        return Integer.MAX_VALUE; // hive has no max limit for strings
      case BINARY:
        return Integer.MAX_VALUE; // hive has no max limit for binary
      case TINYINT:
        return 3;
      case SMALLINT:
        return 5;
      case INTEGER:
        return 10;
      case BIGINT:
        return 19;
      case FLOAT:
        return 7;
      case DOUBLE:
        return 15;
      case DATE:
        return 10;
      case TIMESTAMP:
        return 29;
      case TIMESTAMP_WITH_TIMEZONE:
        return 31;
      case DECIMAL:
        return columnAttributes.precision;
      case OTHER:
      case JAVA_OBJECT:
        {
          switch (tType) {
            case INTERVAL_YEAR_MONTH_TYPE:
              // -yyyyyyy-mm  : should be more than enough
              return 11;
            case INTERVAL_DAY_TIME_TYPE:
              // -ddddddddd hh:mm:ss.nnnnnnnnn
              return 29;
            default:
              return Integer.MAX_VALUE;
          }
        }
      case ARRAY:
      case STRUCT:
        return Integer.MAX_VALUE;
      default:
        throw new KyuubiSQLException("Invalid column type: " + columnType);
    }
  }

  static int columnScale(int columnType, JdbcColumnAttributes columnAttributes)
      throws SQLException {
    switch (columnType) {
      case NULL:
      case BOOLEAN:
      case CHAR:
      case VARCHAR:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case DATE:
      case BINARY:
        return 0;
      case FLOAT:
        return 7;
      case DOUBLE:
        return 15;
      case TIMESTAMP:
      case TIMESTAMP_WITH_TIMEZONE:
        return 9;
      case DECIMAL:
        return columnAttributes.scale;
      case OTHER:
      case JAVA_OBJECT:
      case ARRAY:
      case STRUCT:
        return 0;
      default:
        throw new KyuubiSQLException("Invalid column type: " + columnType);
    }
  }
}
