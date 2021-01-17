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

package org.apache.kyuubi.engine.spark.operation


import java.util.regex.Pattern

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, CalendarIntervalType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, NullType, NumericType, ShortType, StringType, StructField, StructType, TimestampType}

import org.apache.kyuubi.operation.OperationType
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.session.Session

class GetColumns(
    spark: SparkSession,
    session: Session,
    catalogName: String,
    schemaName: String,
    tableName: String,
    columnName: String)
  extends SparkOperation(spark, OperationType.GET_COLUMNS, session) {

  private def toJavaSQLType(typ: DataType): Int = typ match {
    case NullType => java.sql.Types.NULL
    case BooleanType => java.sql.Types.BOOLEAN
    case ByteType => java.sql.Types.TINYINT
    case ShortType => java.sql.Types.SMALLINT
    case IntegerType => java.sql.Types.INTEGER
    case LongType => java.sql.Types.BIGINT
    case FloatType => java.sql.Types.FLOAT
    case DoubleType => java.sql.Types.DOUBLE
    case StringType => java.sql.Types.VARCHAR
    case _: DecimalType => java.sql.Types.DECIMAL
    case DateType => java.sql.Types.DATE
    case TimestampType => java.sql.Types.TIMESTAMP
    case BinaryType => java.sql.Types.BINARY
    case _: ArrayType => java.sql.Types.ARRAY
    case _: MapType => java.sql.Types.JAVA_OBJECT
    case _: StructType => java.sql.Types.STRUCT
    case _ => java.sql.Types.OTHER
  }

  /**
   * For boolean, numeric and datetime types, it returns the default size of its catalyst type
   * For struct type, when its elements are fixed-size, the summation of all element sizes will be
   * returned.
   * For array, map, string, and binaries, the column size is variable, return null as unknown.
   */
  private def getColumnSize(typ: DataType): Option[Int] = typ match {
    case dt @ (BooleanType | _: NumericType | DateType | TimestampType |
               CalendarIntervalType | NullType) =>
      Some(dt.defaultSize)
    case StructType(fields) =>
      val sizeArr = fields.map(f => getColumnSize(f.dataType))
      if (sizeArr.contains(None)) {
        None
      } else {
        Some(sizeArr.map(_.get).sum)
      }
    case _ => None
  }

  /**
   * The number of fractional digits for this type.
   * Null is returned for data types where this is not applicable.
   * For boolean and integrals, the decimal digits is 0
   * For floating types, we follow the IEEE Standard for Floating-Point Arithmetic (IEEE 754)
   * For timestamp values, we support microseconds
   * For decimals, it returns the scale
   */
  private def getDecimalDigits(typ: DataType): Option[Int] = typ match {
    case BooleanType | _: IntegerType => Some(0)
    case FloatType => Some(7)
    case DoubleType => Some(15)
    case d: DecimalType => Some(d.scale)
    case TimestampType => Some(6)
    case _ => None
  }

  private def getNumPrecRadix(typ: DataType): Option[Int] = typ match {
    case _: NumericType => Some(10)
    case _ => None
  }

  private def toRow(db: String, table: String, col: StructField, pos: Int): Row = {
    Row(
      null,                                    // TABLE_CAT
      db,                                      // TABLE_SCHEM
      table,                                   // TABLE_NAME
      col.name,                                // COLUMN_NAME
      toJavaSQLType(col.dataType),             // DATA_TYPE
      col.dataType.sql,                        // TYPE_NAME
      getColumnSize(col.dataType).orNull,      // COLUMN_SIZE
      null,                                    // BUFFER_LENGTH
      getDecimalDigits(col.dataType).orNull,   // DECIMAL_DIGITS
      getNumPrecRadix(col.dataType).orNull,    // NUM_PREC_RADIX
      if (col.nullable) 1 else 0,              // NULLABLE
      col.getComment().getOrElse(""),          // REMARKS
      null,                                    // COLUMN_DEF
      null,                                    // SQL_DATA_TYPE
      null,                                    // SQL_DATETIME_SUB
      null,                                    // CHAR_OCTET_LENGTH
      pos,                                     // ORDINAL_POSITION
      "YES",                                   // IS_NULLABLE
      null,                                    // SCOPE_CATALOG
      null,                                    // SCOPE_SCHEMA
      null,                                    // SCOPE_TABLE
      null,                                    // SOURCE_DATA_TYPE
      "NO"                                     // IS_AUTO_INCREMENT
    )
  }
  override protected def resultSchema: StructType = {
    new StructType()
      .add(TABLE_CAT, "string", nullable = true, "Catalog name. NULL if not applicable")
      .add(TABLE_SCHEM, "string", nullable = true, "Schema name")
      .add(TABLE_NAME, "string", nullable = true, "Table name")
      .add(COLUMN_NAME, "string", nullable = true, "Column name")
      .add(DATA_TYPE, "int", nullable = true, "SQL type from java.sql.Types")
      .add(TYPE_NAME, "string", nullable = true, "Data source dependent type name, for a UDT" +
        " the type name is fully qualified")
      .add(COLUMN_SIZE, "int", nullable = true, "Column size. For char or date types this is" +
        " the maximum number of characters, for numeric or decimal types this is precision.")
      .add(BUFFER_LENGTH, "tinyint", nullable = true, "Unused")
      .add(DECIMAL_DIGITS, "int", nullable = true, "he number of fractional digits")
      .add(NUM_PREC_RADIX, "int", nullable = true, "Radix (typically either 10 or 2)")
      .add(NULLABLE, "int", nullable = true, "Is NULL allowed")
      .add(REMARKS, "string", nullable = true, "Comment describing column (may be null)")
      .add(COLUMN_DEF, "string", nullable = true, "Default value (may be null)")
      .add(SQL_DATA_TYPE, "int", nullable = true, "Unused")
      .add(SQL_DATETIME_SUB, "int", nullable = true, "Unused")
      .add(CHAR_OCTET_LENGTH, "int", nullable = true,
        "For char types the maximum number of bytes in the column")
      .add(ORDINAL_POSITION, "int", nullable = true, "Index of column in table (starting at 1)")
      .add(IS_NULLABLE, "string", nullable = true,
        "'NO' means column definitely does not allow NULL values; 'YES' means the column might" +
          " allow NULL values. An empty string means nobody knows.")
      .add(SCOPE_CATALOG, "string", nullable = true,
        "Catalog of table that is the scope of a reference attribute "
          + "(null if DATA_TYPE isn't REF)")
      .add(SCOPE_SCHEMA, "string", nullable = true,
        "Schema of table that is the scope of a reference attribute "
          + "(null if the DATA_TYPE isn't REF)")
      .add(SCOPE_TABLE, "string", nullable = true,
        "Table name that this the scope of a reference attribute "
          + "(null if the DATA_TYPE isn't REF)")
      .add(SOURCE_DATA_TYPE, "smallint", nullable = true,
        "Source type of a distinct type or user-generated Ref type, "
          + "SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)")
      .add(IS_AUTO_INCREMENT, "string", nullable = true,
        "Indicates whether this column is auto incremented.")
  }

  override protected def runInternal(): Unit = {
    try {
      val catalog = spark.sessionState.catalog
      val schemaPattern = convertSchemaPattern(schemaName)
      val tablePattern = convertIdentifierPattern(tableName, datanucleusFormat = true)
      val columnPattern =
        Pattern.compile(convertIdentifierPattern(columnName, datanucleusFormat = false))
      val tables: Seq[Row] = catalog.listDatabases(schemaPattern).flatMap { db =>
        val identifiers =
          catalog.listTables(db, tablePattern, includeLocalTempViews = false)
        catalog.getTablesByName(identifiers).flatMap { t =>
          t.schema.zipWithIndex
            .filter { f => columnPattern.matcher(f._1.name).matches() }
            .map { case (f, i) => toRow(t.database, t.identifier.table, f, i)
          }
        }
      }

      val gviews = new ArrayBuffer[Row]()
      val globalTmpDb = catalog.globalTempViewManager.database
      if (StringUtils.isEmpty(schemaName) || schemaName == "*"
        || Pattern.compile(convertSchemaPattern(schemaName, false))
        .matcher(globalTmpDb).matches()) {
        catalog.globalTempViewManager.listViewNames(tablePattern).foreach { v =>
          catalog.globalTempViewManager.get(v).foreach { plan =>
            plan.schema.zipWithIndex
              .filter { f => columnPattern.matcher(f._1.name).matches() }
              .foreach { case (f, i) => gviews += toRow(globalTmpDb, v, f, i) }
          }
        }
      }

      val views: Seq[Row] = catalog.listLocalTempViews(tablePattern)
        .map(v => (v, catalog.getTempView(v.table).get))
        .flatMap { case (v, plan) =>
          plan.schema.zipWithIndex
            .filter(f => columnPattern.matcher(f._1.name).matches())
            .map { case (f, i) => toRow(null, v.table, f, i) }
      }

      iter = (tables ++ gviews ++ views).toList.iterator
    } catch {
      onError()
    }
  }
}
