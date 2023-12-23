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

package org.apache.kyuubi.spark.connector.tpcds

import java.lang.{Iterable => JIterable}
import java.lang.reflect.InvocationTargetException
import java.util.{Iterator => JIterator}

import com.google.common.collect.AbstractIterator
import io.trino.tpcds._
import io.trino.tpcds.`type`.{Decimal => TPCDSDecimal}
import io.trino.tpcds.row.generator.RowGenerator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, RebaseDateTime}
import org.apache.spark.sql.types.{CharType, DateType, Decimal, DecimalType, IntegerType, LongType, StringType, StructType, VarcharType}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.kyuubi.spark.connector.tpcds.KyuubiResultsIterator.{FALSE_STRING, TRUE_STRING}
import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTableRows

class KyuubiTPCDSResults(
    val table: Table,
    val startingRowNumber: Long,
    val rowCount: Long,
    val session: Session,
    val schema: StructType) extends JIterable[InternalRow] {

  override def iterator: JIterator[InternalRow] =
    new KyuubiResultsIterator(table, startingRowNumber, rowCount, session, schema)
}

object KyuubiTPCDSResults {
  def constructResults(table: Table, session: Session, schema: StructType): KyuubiTPCDSResults = {
    val chunkBoundaries = io.trino.tpcds.Parallel.splitWork(table, session)
    new KyuubiTPCDSResults(
      table,
      chunkBoundaries.getFirstRow(),
      chunkBoundaries.getLastRow(),
      session,
      schema)
  }
}

class KyuubiResultsIterator(
    val table: Table,
    val startingRowNumber: Long,
    val endingRowNumber: Long,
    val session: Session,
    val sparkSchema: StructType) extends AbstractIterator[InternalRow] {
  private var rowNumber: Long = 0L
  private var rowGenerator: RowGenerator = _
  private var parentRowGenerator: Option[RowGenerator] = None
  private var childRowGenerator: Option[RowGenerator] = None

  try {
    require(table != null, "table is null")
    require(session != null, "session is null")
    require(startingRowNumber >= 1, s"starting row number is less than 1: $startingRowNumber")
    require(
      endingRowNumber <= session.getScaling.getRowCount(table),
      s"starting row number is greater than the total rows in $table: $endingRowNumber")
    rowNumber = startingRowNumber
    rowGenerator = table.getRowGeneratorClass().getDeclaredConstructor().newInstance()
    parentRowGenerator = if (table.isChild()) {
      Some(table.getParent().getRowGeneratorClass().getDeclaredConstructor().newInstance())
    } else None
    childRowGenerator = if (table.hasChild()) {
      Some(table.getChild().getRowGeneratorClass().getDeclaredConstructor().newInstance())
    } else None
  } catch {
    case e @ (_: NoSuchMethodException |
        _: InstantiationException |
        _: InvocationTargetException |
        _: IllegalAccessException) =>
      throw new TpcdsException(e.toString());
  }
  skipRowsUntilStartingRowNumber(startingRowNumber)

  private def skipRowsUntilStartingRowNumber(startingRowNumber: Long): Unit = {
    rowGenerator.skipRowsUntilStartingRowNumber(startingRowNumber)
    parentRowGenerator.foreach(_.skipRowsUntilStartingRowNumber(startingRowNumber))
    childRowGenerator.foreach(_.skipRowsUntilStartingRowNumber(startingRowNumber))
  }

  override protected def computeNext(): InternalRow = {
    if (rowNumber > endingRowNumber) {
      return endOfData
    }
    val result = rowGenerator.generateRowAndChildRows(
      rowNumber,
      session,
      parentRowGenerator.orNull,
      childRowGenerator.orNull)
    var row: InternalRow = null
    if (!result.getRowAndChildRows.isEmpty) {
      row = toInternalRow(KyuubiTableRows.getValues(result.getRowAndChildRows.get(0)))
    }

    if (result.shouldEndRow) {
      rowStop()
      rowNumber += 1
    }
    if (result.getRowAndChildRows().isEmpty()) {
      row = computeNext()
    }
    row
  }

  private def rowStop(): Unit = {
    rowGenerator.consumeRemainingSeedsForRow()
    parentRowGenerator.foreach(_.consumeRemainingSeedsForRow())
    childRowGenerator.foreach(_.consumeRemainingSeedsForRow())
  }

  private val reusedRow = new Array[Any](sparkSchema.length)

  def toInternalRow(values: Array[Any]): InternalRow = {
    var i = 0
    while (i < values.length) {
      reusedRow(i) = (values(i), sparkSchema(i).dataType) match {
        case (None | null, _) => null
        case (Some(Options.DEFAULT_NULL_STRING), _) => null
        case (Some(v: Boolean), _) => if (v) TRUE_STRING else FALSE_STRING
        case (Some(v: Int), IntegerType) => v
        case (Some(v: Long), IntegerType) => v.toInt
        case (Some(v: Int), LongType) => v.toLong
        case (Some(v: Long), LongType) => v
        case (Some(v: Long), DateType) =>
          RebaseDateTime.rebaseJulianToGregorianDays(v.toInt) - DateTimeUtils.JULIAN_DAY_OF_EPOCH
        case (Some(v), StringType) => UTF8String.fromString(v.toString)
        case (Some(v), CharType(_)) => UTF8String.fromString(v.toString)
        case (Some(v), VarcharType(_)) => UTF8String.fromString(v.toString)
        case (Some(v: TPCDSDecimal), t: DecimalType) =>
          Decimal(v.getNumber, t.precision, t.scale)
        case (Some(v: Int), t: DecimalType) =>
          val decimal = Decimal(v)
          decimal.changePrecision(t.precision, t.scale)
          decimal
        case (Some(v), dt) => throw new IllegalArgumentException(
            s"value: $v, value class: ${v.getClass.getName} type: $dt")
      }
      i += 1
    }
    new GenericInternalRow(reusedRow)
  }
}

object KyuubiResultsIterator {
  private val TRUE_STRING = UTF8String.fromString("Y")
  private val FALSE_STRING = UTF8String.fromString("N")
}
