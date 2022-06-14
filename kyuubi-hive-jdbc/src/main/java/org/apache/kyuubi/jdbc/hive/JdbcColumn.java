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
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.TimestampTZ;

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

  static String columnClassName(String typeStr, JdbcColumnAttributes columnAttributes)
      throws SQLException {
    int columnType = typeStringToSqlType(typeStr);
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
          switch (typeStr) {
            case "INTERVAL_YEAR_MONTH":
              return HiveIntervalYearMonth.class.getName();
            case "INTERVAL_DAY_TIME":
              return HiveIntervalDayTime.class.getName();
            default:
              return String.class.getName();
          }
        }
      case ARRAY:
      case STRUCT:
        return String.class.getName();
      default:
        throw new SQLException("Invalid column type: " + columnType);
    }
  }

  public static int typeStringToSqlType(String type) throws SQLException {
    if ("string".equalsIgnoreCase(type)) {
      return VARCHAR;
    } else if ("varchar".equalsIgnoreCase(type)) {
      return VARCHAR;
    } else if ("char".equalsIgnoreCase(type)) {
      return CHAR;
    } else if ("float".equalsIgnoreCase(type)) {
      return FLOAT;
    } else if ("double".equalsIgnoreCase(type)) {
      return DOUBLE;
    } else if ("boolean".equalsIgnoreCase(type)) {
      return BOOLEAN;
    } else if ("tinyint".equalsIgnoreCase(type)) {
      return TINYINT;
    } else if ("smallint".equalsIgnoreCase(type)) {
      return SMALLINT;
    } else if ("int".equalsIgnoreCase(type)) {
      return INTEGER;
    } else if ("bigint".equalsIgnoreCase(type)) {
      return BIGINT;
    } else if ("date".equalsIgnoreCase(type)) {
      return DATE;
    } else if ("timestamp".equalsIgnoreCase(type)) {
      return TIMESTAMP;
    } else if ("timestamp with local time zone".equalsIgnoreCase(type)) {
      return OTHER;
    } else if ("interval_year_month".equalsIgnoreCase(type)) {
      return OTHER;
    } else if ("interval_day_time".equalsIgnoreCase(type)) {
      return OTHER;
    } else if ("decimal".equalsIgnoreCase(type)) {
      return DECIMAL;
    } else if ("binary".equalsIgnoreCase(type)) {
      return BINARY;
    } else if ("map".equalsIgnoreCase(type)) {
      return JAVA_OBJECT;
    } else if ("array".equalsIgnoreCase(type)) {
      return ARRAY;
    } else if ("struct".equalsIgnoreCase(type)) {
      return STRUCT;
    } else if ("uniontype".equalsIgnoreCase(type)) {
      return OTHER;
    } else if ("void".equalsIgnoreCase(type) || "null".equalsIgnoreCase(type)) {
      return NULL;
    }
    throw new SQLException("Unrecognized column type: " + type);
  }

  static String getColumnTypeName(String type) throws SQLException {
    // TODO: this would be better handled in an enum
    if ("string".equalsIgnoreCase(type)) {
      return "string";
    } else if ("varchar".equalsIgnoreCase(type)) {
      return "varchar";
    } else if ("char".equalsIgnoreCase(type)) {
      return "char";
    } else if ("float".equalsIgnoreCase(type)) {
      return "float";
    } else if ("double".equalsIgnoreCase(type)) {
      return "double";
    } else if ("boolean".equalsIgnoreCase(type)) {
      return "boolean";
    } else if ("tinyint".equalsIgnoreCase(type)) {
      return "tinyint";
    } else if ("smallint".equalsIgnoreCase(type)) {
      return "smallint";
    } else if ("int".equalsIgnoreCase(type)) {
      return "int";
    } else if ("bigint".equalsIgnoreCase(type)) {
      return "bigint";
    } else if ("timestamp".equalsIgnoreCase(type)) {
      return "timestamp";
    } else if ("timestamp with local time zone".equalsIgnoreCase(type)) {
      return "timestamp with local time zone";
    } else if ("date".equalsIgnoreCase(type)) {
      return "date";
    } else if ("interval_year_month".equalsIgnoreCase(type)) {
      return "interval_year_month";
    } else if ("interval_day_time".equalsIgnoreCase(type)) {
      return "interval_day_time";
    } else if ("decimal".equalsIgnoreCase(type)) {
      return "decimal";
    } else if ("binary".equalsIgnoreCase(type)) {
      return "binary";
    } else if ("void".equalsIgnoreCase(type) || "null".equalsIgnoreCase(type)) {
      return "void";
    } else if (type.equalsIgnoreCase("map")) {
      return "map";
    } else if (type.equalsIgnoreCase("array")) {
      return "array";
    } else if (type.equalsIgnoreCase("struct")) {
      return "struct";
    }

    throw new SQLException("Unrecognized column type: " + type);
  }

  static int columnDisplaySize(String typeStr, JdbcColumnAttributes columnAttributes)
      throws SQLException {
    int columnType = typeStringToSqlType(typeStr);
    switch (columnType) {
      case NULL:
        return 4; // "NULL"
      case BOOLEAN:
        return columnPrecision(typeStr, columnAttributes);
      case CHAR:
      case VARCHAR:
        return columnPrecision(typeStr, columnAttributes);
      case BINARY:
        return Integer.MAX_VALUE; // hive has no max limit for binary
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return columnPrecision(typeStr, columnAttributes) + 1; // allow +/-
      case DATE:
        return 10;
      case TIMESTAMP:
      case TIMESTAMP_WITH_TIMEZONE:
        return columnPrecision(typeStr, columnAttributes);

        // see
        // http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Float.MAX_EXPONENT
      case FLOAT:
        return 24; // e.g. -(17#).e-###
        // see
        // http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Double.MAX_EXPONENT
      case DOUBLE:
        return 25; // e.g. -(17#).e-####
      case DECIMAL:
        return columnPrecision(typeStr, columnAttributes) + 2; // '-' sign and '.'
      case OTHER:
      case JAVA_OBJECT:
        return columnPrecision(typeStr, columnAttributes);
      case ARRAY:
      case STRUCT:
        return Integer.MAX_VALUE;
      default:
        throw new SQLException("Invalid column type: " + columnType);
    }
  }

  static int columnPrecision(String typeStr, JdbcColumnAttributes columnAttributes)
      throws SQLException {
    int columnType = typeStringToSqlType(typeStr);
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
          switch (typeStr) {
            case "INTERVAL_YEAR_MONTH":
              // -yyyyyyy-mm  : should be more than enough
              return 11;
            case "INTERVAL_DAY_TIME":
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
        throw new SQLException("Invalid column type: " + columnType);
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
        throw new SQLException("Invalid column type: " + columnType);
    }
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

  public String getComment() {
    return comment;
  }

  public int getOrdinalPos() {
    return ordinalPos;
  }
}
