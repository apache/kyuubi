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

package org.apache.kyuubi.server.mysql.constant

import java.sql.Types

import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId

sealed abstract class MySQLDataType(val value: Int)

// https://dev.mysql.com/doc/internals/en/com-query-response.html#column-type
object MySQLDataType {
  object DECIMAL extends MySQLDataType(0x00)

  object TINY extends MySQLDataType(0x01)

  object SHORT extends MySQLDataType(0x02)

  object LONG extends MySQLDataType(0x03)

  object FLOAT extends MySQLDataType(0x04)

  object DOUBLE extends MySQLDataType(0x05)

  object NULL extends MySQLDataType(0x06)

  object TIMESTAMP extends MySQLDataType(0x07)

  object LONGLONG extends MySQLDataType(0x08)

  object INT24 extends MySQLDataType(0x09)

  object DATE extends MySQLDataType(0x0A)

  object TIME extends MySQLDataType(0x0B)

  object DATETIME extends MySQLDataType(0x0C)

  object YEAR extends MySQLDataType(0x0D)

  // Internal to MySQL Server
  object NEWDATE extends MySQLDataType(0x0E)

  object VARCHAR extends MySQLDataType(0x0F)

  object BIT extends MySQLDataType(0x10)

  // Internal to MySQL Server
  object TIMESTAMP2 extends MySQLDataType(0x11)

  // Internal to MySQL Server
  object DATETIME2 extends MySQLDataType(0x12)

  // Internal to MySQL Server
  object TIME2 extends MySQLDataType(0x13)

  // Do not describe in document, but actual exist.
  // https://github.com/apache/shardingsphere/issues/4795
  object JSON extends MySQLDataType(0xF5)

  object NEWDECIMAL extends MySQLDataType(0xF6)

  object ENUM extends MySQLDataType(0xF7)

  object SET extends MySQLDataType(0xF8)

  object TINY_BLOB extends MySQLDataType(0xF9)

  object MEDIUM_BLOB extends MySQLDataType(0xFA)

  object LONG_BLOB extends MySQLDataType(0xFB)

  object BLOB extends MySQLDataType(0xFC)

  object VAR_STRING extends MySQLDataType(0xFD)

  object STRING extends MySQLDataType(0xFE)

  object GEOMETRY extends MySQLDataType(0xFF)

  def valueOf(value: Int): MySQLDataType = value match {
    case 0x00 => DECIMAL
    case 0x01 => TINY
    case 0x02 => SHORT
    case 0x03 => LONG
    case 0x04 => FLOAT
    case 0x05 => DOUBLE
    case 0x06 => NULL
    case 0x07 => TIMESTAMP
    case 0x08 => LONGLONG
    case 0x09 => INT24
    case 0x0A => DATE
    case 0x0B => TIME
    case 0x0C => DATETIME
    case 0x0D => YEAR
    case 0x0E => NEWDATE
    case 0x0F => VARCHAR
    case 0x10 => BIT
    case 0x11 => TIMESTAMP2
    case 0x12 => DATETIME2
    case 0x13 => TIME2
    case 0xF5 => JSON
    case 0xF6 => NEWDECIMAL
    case 0xF7 => ENUM
    case 0xF8 => SET
    case 0xF9 => TINY_BLOB
    case 0xFA => MEDIUM_BLOB
    case 0xFB => LONG_BLOB
    case 0xFC => BLOB
    case 0xFD => VAR_STRING
    case 0xFE => STRING
    case 0xFF => GEOMETRY
    case other => throw new IllegalArgumentException(
        s"Illegal value $other of MySQLDataType")
  }

  def valueOfJdbcType(jdbcValue: Int): MySQLDataType = jdbcValue match {
    case Types.BIT => BIT
    case Types.TINYINT => TINY
    case Types.SMALLINT => SHORT
    case Types.INTEGER => LONG
    case Types.BIGINT => LONGLONG
    case Types.FLOAT => FLOAT
    case Types.REAL => FLOAT
    case Types.DOUBLE => DOUBLE
    case Types.NUMERIC => NEWDECIMAL
    case Types.DECIMAL => NEWDECIMAL
    case Types.CHAR => STRING
    case Types.VARCHAR => VAR_STRING
    case Types.LONGVARCHAR => VAR_STRING
    case Types.DATE => DATE
    case Types.TIME => TIME
    case Types.TIMESTAMP => TIMESTAMP
    case Types.BINARY => STRING
    case Types.VARBINARY => VAR_STRING
    case Types.LONGVARBINARY => VAR_STRING
    case Types.NULL => NULL
    case Types.BLOB => BLOB
    case other => throw new IllegalArgumentException(
        s"Illegal JDBC type value $other of MySQLDataType")
  }

  def valueOfThriftType(tType: TTypeId): MySQLDataType = tType match {
    case TTypeId.BOOLEAN_TYPE => TINY
    case TTypeId.TINYINT_TYPE => TINY
    case TTypeId.SMALLINT_TYPE => SHORT
    case TTypeId.INT_TYPE => LONG
    case TTypeId.BIGINT_TYPE => LONGLONG
    case TTypeId.FLOAT_TYPE => FLOAT
    case TTypeId.DOUBLE_TYPE => DOUBLE
    case TTypeId.STRING_TYPE => VAR_STRING
    case TTypeId.TIMESTAMP_TYPE => TIMESTAMP
    case TTypeId.BINARY_TYPE => STRING
    case TTypeId.ARRAY_TYPE => VAR_STRING // not exactly match, fallback
    case TTypeId.MAP_TYPE => VAR_STRING // not exactly match, fallback
    case TTypeId.STRUCT_TYPE => VAR_STRING // not exactly match, fallback
    case TTypeId.UNION_TYPE => VAR_STRING // not exactly match, fallback
    case TTypeId.USER_DEFINED_TYPE => VAR_STRING // not exactly match, fallback
    case TTypeId.DECIMAL_TYPE => NEWDECIMAL
    case TTypeId.NULL_TYPE => NULL
    case TTypeId.DATE_TYPE => DATE
    case TTypeId.VARCHAR_TYPE => VAR_STRING
    case TTypeId.CHAR_TYPE => STRING
    case TTypeId.INTERVAL_YEAR_MONTH_TYPE => VAR_STRING // not exactly match, fallback
    case TTypeId.INTERVAL_DAY_TIME_TYPE => VAR_STRING // not exactly match, fallback
    case TTypeId.TIMESTAMPLOCALTZ_TYPE => TIMESTAMP
  }
}
