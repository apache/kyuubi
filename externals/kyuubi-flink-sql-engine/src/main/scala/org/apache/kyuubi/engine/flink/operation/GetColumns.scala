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

package org.apache.kyuubi.engine.flink.operation

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.apache.commons.lang3.StringUtils
import org.apache.flink.table.api.{DataTypes, ResultKind}
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.catalog.{Column, ObjectIdentifier}
import org.apache.flink.table.types.logical._
import org.apache.flink.types.Row

import org.apache.kyuubi.engine.flink.result.ResultSet
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.session.Session

class GetColumns(
    session: Session,
    catalogNameOrEmpty: String,
    schemaNamePattern: String,
    tableNamePattern: String,
    columnNamePattern: String)
  extends FlinkOperation(session) {

  override protected def runInternal(): Unit = {
    try {
      val tableEnv = sessionContext.getExecutionContext.getTableEnvironment
      val resolver = tableEnv.asInstanceOf[StreamTableEnvironmentImpl]
        .getCatalogManager.getSchemaResolver

      val catalogName =
        if (StringUtils.isEmpty(catalogNameOrEmpty)) tableEnv.getCurrentCatalog
        else catalogNameOrEmpty

      val schemaNameRegex = toJavaRegex(schemaNamePattern).r
      val tableNameRegex = toJavaRegex(tableNamePattern).r
      val columnNameRegex = toJavaRegex(columnNamePattern).r

      val columns = tableEnv.getCatalog(catalogName).asScala.toArray.flatMap { flinkCatalog =>
        flinkCatalog.listDatabases().asScala
          .filter { schemaName => schemaNameRegex.pattern.matcher(schemaName).matches() }
          .flatMap { schemaName =>
            flinkCatalog.listTables(schemaName).asScala
              .filter { tableName => tableNameRegex.pattern.matcher(tableName).matches() }
              .map { tableName =>
                val objPath = ObjectIdentifier.of(catalogName, schemaName, tableName).toObjectPath
                Try(flinkCatalog.getTable(objPath)) match {
                  case Success(flinkTable) => (tableName, Some(flinkTable))
                  case Failure(_) => (tableName, None)
                }
              }
              .filter {
                case (_, None) => false
                case _ => true
              }.flatMap { case (tableName, flinkTable) =>
                val schema = flinkTable.get.getUnresolvedSchema.resolve(resolver)
                val rows = schema.getColumns.asScala.toArray.zipWithIndex
                  .filter(c =>
                    columnNameRegex.pattern.matcher(c._1.getName).matches())
                  .map { case (column, pos) =>
                    val logicalType = column.getDataType.getLogicalType
                    Row.of(
                      catalogName, // TABLE_CAT
                      schemaName, // TABLE_SCHEM
                      tableName, // TABLE_NAME
                      column.getName, // COLUMN_NAME
                      Integer.valueOf(toJavaSQLType(logicalType)), // DATA_TYPE
                      logicalType.toString.replace(" NOT NULL", ""), // TYPE_NAME
                      null, // COLUMN_SIZE
                      null, // BUFFER_LENGTH
                      getDecimalDigits(logicalType), // DECIMAL_DIGITS
                      getNumPrecRadix(logicalType), // NUM_PREC_RADIX
                      Integer.valueOf(if (logicalType.isNullable) 1 else 0), // NULLABLE
                      column.getComment.orElse(""), // REMARKS
                      null, // COLUMN_DEF
                      null, // SQL_DATA_TYPE
                      null, // SQL_DATETIME_SUB
                      null, // CHAR_OCTET_LENGTH
                      Integer.valueOf(pos), // ORDINAL_POSITION
                      "YES", // IS_NULLABLE
                      null, // SCOPE_CATALOG
                      null, // SCOPE_SCHEMA
                      null, // SCOPE_TABLE
                      null, // SOURCE_DATA_TYPE
                      "NO" // IS_AUTO_INCREMENT
                    )
                  }
                rows
              }
          }
      }

      resultSet = ResultSet.builder.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
        .columns(
          Column.physical(TABLE_CAT, DataTypes.STRING),
          Column.physical(TABLE_SCHEM, DataTypes.STRING),
          Column.physical(TABLE_NAME, DataTypes.STRING),
          Column.physical(COLUMN_NAME, DataTypes.STRING),
          Column.physical(DATA_TYPE, DataTypes.INT),
          Column.physical(TYPE_NAME, DataTypes.STRING),
          Column.physical(COLUMN_SIZE, DataTypes.INT),
          Column.physical(BUFFER_LENGTH, DataTypes.TINYINT),
          Column.physical(DECIMAL_DIGITS, DataTypes.INT),
          Column.physical(NUM_PREC_RADIX, DataTypes.INT),
          Column.physical(NULLABLE, DataTypes.INT),
          Column.physical(REMARKS, DataTypes.STRING),
          Column.physical(COLUMN_DEF, DataTypes.STRING),
          Column.physical(SQL_DATA_TYPE, DataTypes.INT),
          Column.physical(SQL_DATETIME_SUB, DataTypes.INT),
          Column.physical(CHAR_OCTET_LENGTH, DataTypes.INT),
          Column.physical(ORDINAL_POSITION, DataTypes.INT),
          Column.physical(IS_NULLABLE, DataTypes.STRING),
          Column.physical(SCOPE_CATALOG, DataTypes.STRING),
          Column.physical(SCOPE_SCHEMA, DataTypes.STRING),
          Column.physical(SCOPE_TABLE, DataTypes.STRING),
          Column.physical(SOURCE_DATA_TYPE, DataTypes.SMALLINT),
          Column.physical(IS_AUTO_INCREMENT, DataTypes.STRING))
        .data(columns)
        .build
    } catch onError()
  }

  private def toJavaSQLType(flinkType: LogicalType): Int = flinkType.getClass match {
    case c: Class[_] if c == classOf[NullType] => java.sql.Types.NULL
    case c: Class[_] if c == classOf[BooleanType] => java.sql.Types.BOOLEAN
    case c: Class[_] if c == classOf[TinyIntType] => java.sql.Types.TINYINT
    case c: Class[_] if c == classOf[SmallIntType] => java.sql.Types.SMALLINT
    case c: Class[_] if c == classOf[IntType] => java.sql.Types.INTEGER
    case c: Class[_] if c == classOf[BigIntType] => java.sql.Types.BIGINT
    case c: Class[_] if c == classOf[FloatType] => java.sql.Types.FLOAT
    case c: Class[_] if c == classOf[DoubleType] => java.sql.Types.DOUBLE
    case c: Class[_] if c == classOf[CharType] => java.sql.Types.VARCHAR
    case c: Class[_] if c == classOf[VarCharType] => java.sql.Types.VARCHAR
    case c: Class[_] if c == classOf[DecimalType] => java.sql.Types.DECIMAL
    case c: Class[_] if c == classOf[DateType] => java.sql.Types.DATE
    case c: Class[_] if c == classOf[TimestampType] => java.sql.Types.TIMESTAMP
    case c: Class[_] if c == classOf[DayTimeIntervalType] => java.sql.Types.TIMESTAMP
    case c: Class[_] if c == classOf[YearMonthIntervalType] => java.sql.Types.TIMESTAMP
    case c: Class[_] if c == classOf[ZonedTimestampType] => java.sql.Types.TIMESTAMP
    case c: Class[_] if c == classOf[TimeType] => java.sql.Types.TIME
    case c: Class[_] if c == classOf[BinaryType] => java.sql.Types.BINARY
    case c: Class[_] if c == classOf[VarBinaryType] => java.sql.Types.BINARY
    case c: Class[_] if c == classOf[ArrayType] => java.sql.Types.ARRAY
    case c: Class[_] if c == classOf[MapType] => java.sql.Types.JAVA_OBJECT
    case c: Class[_] if c == classOf[MultisetType] => java.sql.Types.JAVA_OBJECT
    case c: Class[_] if c == classOf[StructuredType] => java.sql.Types.STRUCT
    case c: Class[_] if c == classOf[DistinctType] => java.sql.Types.OTHER
    case c: Class[_] if c == classOf[RawType[_]] => java.sql.Types.OTHER
    case c: Class[_] if c == classOf[RowType] => java.sql.Types.OTHER
    case c: Class[_] if c == classOf[SymbolType[_]] => java.sql.Types.OTHER
    case _ => java.sql.Types.OTHER
  }

  private def getDecimalDigits(flinkType: LogicalType): Integer = flinkType.getClass match {
    case c: Class[_] if c == classOf[BooleanType] => 0
    case c: Class[_] if c == classOf[IntType] => 0
    case c: Class[_] if c == classOf[FloatType] => 7
    case c: Class[_] if c == classOf[DoubleType] => 15
    case c: Class[_] if c == classOf[DecimalType] => flinkType.asInstanceOf[DecimalType].getScale
    case c: Class[_] if c == classOf[TimestampType] => 6
    case _ => null
  }

  private def getNumPrecRadix(flinkType: LogicalType): Integer = flinkType.getClass match {
    case c: Class[_] if c == classOf[TinyIntType] => 10
    case c: Class[_] if c == classOf[SmallIntType] => 10
    case c: Class[_] if c == classOf[IntType] => 10
    case c: Class[_] if c == classOf[BigIntType] => 10
    case c: Class[_] if c == classOf[FloatType] => 10
    case c: Class[_] if c == classOf[DoubleType] => 10
    case c: Class[_] if c == classOf[DecimalType] => 10
    case _ => null
  }
}
