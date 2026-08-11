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

package org.apache.kyuubi.server.flight

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, OffsetDateTime, ZonedDateTime}
import java.util.{BitSet, Collections}

import scala.collection.JavaConverters._

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, MessageSerializer}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.util.Text

import org.apache.kyuubi.jdbc.hive.KyuubiArrowQueryResultSet
import org.apache.kyuubi.jdbc.hive.arrow.ArrowUtils
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

/**
 * Shared Kyuubi thrift/Arrow conversion helpers for Flight SQL.
 *
 * Schema and value conversion are engine-agnostic: engines expose either Arrow
 * IPC inside a binary [[TRowSet]] column or ordinary columnar thrift values.
 */
object KyuubiFlightArrowUtils {

  def schemaFromMetadata(metadata: TGetResultSetMetadataResp): Schema = {
    val columns = Option(metadata).flatMap(m => Option(m.getSchema))
      .map(_.getColumns.asScala.toSeq)
      .getOrElse(Seq.empty)
    new Schema(columns.map { column =>
      val arrowType = Option(column.getTypeDesc)
        .flatMap(desc => Option(desc.getTypes).flatMap(_.asScala.headOption))
        .map(typeEntry => arrowTypeFromEntry(typeEntry, column.getColumnName))
        .getOrElse(ArrowType.Utf8.INSTANCE)
      new Field(
        column.getColumnName,
        new FieldType(true, arrowType, null),
        Collections.emptyList[Field]())
    }.asJava)
  }

  private def arrowTypeFromEntry(typeEntry: TTypeEntry, columnName: String): ArrowType = {
    if (!typeEntry.isSetPrimitiveEntry) {
      throw new IllegalArgumentException(
        s"Unsupported non-primitive Flight SQL column type for '$columnName'")
    }
    val primitive = typeEntry.getPrimitiveEntry
    val attributes = KyuubiArrowQueryResultSet.getColumnAttributes(primitive)
    try {
      ArrowUtils.toArrowType(primitive.getType, attributes)
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(
          s"Unsupported Flight SQL column type '${primitive.getType}' for '$columnName'",
          e)
    }
  }

  /**
   * True when the row set carries no data rows.
   *
   * Columnar thrift generators
   * (see [[org.apache.kyuubi.engine.result.TRowSetGenerator.toColumnBasedSet]])
   * still emit one empty [[TColumn]] per schema field when the iterator is exhausted. Checking only
   * `getColumnsSize == 0` would treat those pages as non-empty and can loop forever in
   * [[FlightResultIterator]].
   */
  def isEmpty(rowSet: TRowSet): Boolean = rowCount(rowSet) == 0

  /**
   * Number of data rows in a thrift [[TRowSet]] (columnar values, row-based rows, or Arrow batch).
   */
  def rowCount(rowSet: TRowSet): Int = {
    if (rowSet == null) {
      0
    } else if (rowSet.getColumns != null && rowSet.getColumnsSize > 0) {
      // Arrow IPC is encoded as a single binary value. A present Arrow binary value
      // counts as non-empty at the thrift layer (decoded length may still be 0).
      columnSize(rowSet.getColumns.get(0))
    } else if (rowSet.getRows != null) {
      rowSet.getRowsSize
    } else {
      0
    }
  }

  def isArrowRowSet(rowSet: TRowSet): Boolean =
    rowSet != null &&
      rowSet.getColumnsSize == 1 &&
      rowSet.getColumns.get(0).isSetBinaryVal &&
      rowSet.getColumns.get(0).getBinaryVal.getValuesSize == 1

  def arrowBatchBytes(rowSet: TRowSet): Long = {
    if (!isArrowRowSet(rowSet)) {
      0L
    } else {
      rowSet.getColumns.get(0).getBinaryVal.getValues.get(0).remaining().toLong
    }
  }

  def decodeBatch(rowSet: TRowSet, allocator: BufferAllocator): ArrowRecordBatch = {
    val buffer = rowSet.getColumns.get(0).getBinaryVal.getValues.get(0)
    val bytes = new Array[Byte](buffer.remaining())
    buffer.duplicate().get(bytes)
    MessageSerializer.deserializeRecordBatch(
      new ReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes))),
      allocator)
  }

  /**
   * Populate [[root]] directly from a columnar thrift [[TRowSet]] without first
   * building an intermediate `Seq[Seq[AnyRef]]`.
   */
  def populateRootFromRowSet(root: VectorSchemaRoot, rowSet: TRowSet): Unit = {
    root.clear()
    root.allocateNew()
    if (rowSet == null) {
      root.setRowCount(0)
      return
    }

    if (rowSet.getColumns != null && rowSet.getColumnsSize > 0) {
      val columns = rowSet.getColumns.asScala
      val rowCount = columnSize(columns.head)
      val vectors = root.getFieldVectors.asScala
      val fields = root.getSchema.getFields.asScala
      val paired = columns.zip(vectors.zip(fields)).take(math.min(columns.size, vectors.size))
      paired.foreach { case (column, (vector, field)) =>
        writeColumn(vector, field.getType, column)
        vector.setValueCount(rowCount)
      }
      if (columns.size < vectors.size) {
        vectors.drop(columns.size).foreach { vector =>
          (0 until rowCount).foreach(vector.setNull)
          vector.setValueCount(rowCount)
        }
      }
      root.setRowCount(rowCount)
    } else if (rowSet.getRows != null && !rowSet.getRows.isEmpty) {
      val rows = rowSet.getRows.asScala
      val vectors = root.getFieldVectors.asScala
      val fields = root.getSchema.getFields.asScala
      vectors.zip(fields).zipWithIndex.foreach { case ((vector, field), columnIndex) =>
        rows.zipWithIndex.foreach { case (row, rowIndex) =>
          val value =
            if (row.getColValsSize > columnIndex) row.getColVals.get(columnIndex).getFieldValue
            else null
          setValue(vector, field.getType, rowIndex, value)
        }
        vector.setValueCount(rows.size)
      }
      root.setRowCount(rows.size)
    } else {
      root.setRowCount(0)
    }
  }

  private def writeColumn(vector: FieldVector, arrowType: ArrowType, column: TColumn): Unit = {
    val size = columnSize(column)
    (0 until size).foreach { rowIndex =>
      setValue(vector, arrowType, rowIndex, columnValue(column, rowIndex))
    }
  }

  private def columnSize(column: TColumn): Int = {
    if (column.isSetBoolVal) column.getBoolVal.getValuesSize
    else if (column.isSetByteVal) column.getByteVal.getValuesSize
    else if (column.isSetI16Val) column.getI16Val.getValuesSize
    else if (column.isSetI32Val) column.getI32Val.getValuesSize
    else if (column.isSetI64Val) column.getI64Val.getValuesSize
    else if (column.isSetDoubleVal) column.getDoubleVal.getValuesSize
    else if (column.isSetStringVal) column.getStringVal.getValuesSize
    else if (column.isSetBinaryVal) column.getBinaryVal.getValuesSize
    else 0
  }

  private def columnValue(column: TColumn, rowIndex: Int): AnyRef = {
    def nullsOf(bytes: Array[Byte]): BitSet =
      if (bytes == null) new BitSet() else BitSet.valueOf(bytes)

    if (column.isSetBoolVal) {
      if (nullsOf(column.getBoolVal.getNulls).get(rowIndex)) null
      else column.getBoolVal.getValues.get(rowIndex)
    } else if (column.isSetByteVal) {
      if (nullsOf(column.getByteVal.getNulls).get(rowIndex)) null
      else column.getByteVal.getValues.get(rowIndex)
    } else if (column.isSetI16Val) {
      if (nullsOf(column.getI16Val.getNulls).get(rowIndex)) null
      else column.getI16Val.getValues.get(rowIndex)
    } else if (column.isSetI32Val) {
      if (nullsOf(column.getI32Val.getNulls).get(rowIndex)) null
      else column.getI32Val.getValues.get(rowIndex)
    } else if (column.isSetI64Val) {
      if (nullsOf(column.getI64Val.getNulls).get(rowIndex)) null
      else column.getI64Val.getValues.get(rowIndex)
    } else if (column.isSetDoubleVal) {
      if (nullsOf(column.getDoubleVal.getNulls).get(rowIndex)) null
      else column.getDoubleVal.getValues.get(rowIndex)
    } else if (column.isSetStringVal) {
      if (nullsOf(column.getStringVal.getNulls).get(rowIndex)) null
      else column.getStringVal.getValues.get(rowIndex)
    } else if (column.isSetBinaryVal) {
      if (nullsOf(column.getBinaryVal.getNulls).get(rowIndex)) null
      else column.getBinaryVal.getValues.get(rowIndex)
    } else {
      null
    }
  }

  private def setValue(
      vector: FieldVector,
      arrowType: ArrowType,
      rowIndex: Int,
      value: AnyRef): Unit = {
    if (value == null) {
      vector.setNull(rowIndex)
      return
    }

    arrowType match {
      case _: ArrowType.Utf8 =>
        vector.asInstanceOf[VarCharVector].setSafe(rowIndex, new Text(value.toString))
      case _: ArrowType.Binary =>
        vector.asInstanceOf[VarBinaryVector].setSafe(rowIndex, bytes(value))
      case _: ArrowType.Bool =>
        val bit: Boolean = value match {
          case b: java.lang.Boolean => b.booleanValue()
          case s: String => s.toBoolean
          case _ => number(value).intValue() != 0
        }
        vector.asInstanceOf[BitVector].setSafe(rowIndex, if (bit) 1 else 0)
      case intType: ArrowType.Int =>
        intType.getBitWidth match {
          case 8 => vector.asInstanceOf[TinyIntVector].setSafe(rowIndex, number(value).byteValue())
          case 16 =>
            vector.asInstanceOf[SmallIntVector].setSafe(rowIndex, number(value).shortValue())
          case 32 => vector.asInstanceOf[IntVector].setSafe(rowIndex, number(value).intValue())
          case 64 => vector.asInstanceOf[BigIntVector].setSafe(rowIndex, number(value).longValue())
          case _ => throw new IllegalArgumentException(s"Unsupported integer width $intType")
        }
      case floatingPoint: ArrowType.FloatingPoint =>
        floatingPoint.getPrecision match {
          case FloatingPointPrecision.SINGLE =>
            vector.asInstanceOf[Float4Vector].setSafe(rowIndex, number(value).floatValue())
          case FloatingPointPrecision.DOUBLE =>
            vector.asInstanceOf[Float8Vector].setSafe(rowIndex, number(value).doubleValue())
          case _ =>
            throw new IllegalArgumentException(
              s"Unsupported floating point type $arrowType")
        }
      case _: ArrowType.Decimal =>
        vector.asInstanceOf[DecimalVector].setSafe(
          rowIndex,
          new java.math.BigDecimal(value.toString))
      case _: ArrowType.Date =>
        val days = value match {
          case d: Date => (d.getTime / (24L * 60L * 60L * 1000L)).toInt
          case ld: LocalDate => ld.toEpochDay.toInt
          case _ => number(value).intValue()
        }
        vector.asInstanceOf[DateDayVector].setSafe(rowIndex, days)
      case ts: ArrowType.Timestamp =>
        val micros = timestampMicros(value)
        vector.asInstanceOf[TimeStampVector].setSafe(rowIndex, micros)
        // Preserve timezone metadata already present on the Arrow field type.
        val _ = ts
      case _: ArrowType.Null =>
        vector.setNull(rowIndex)
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported Arrow type $other for Flight SQL value conversion")
    }
  }

  private def timestampMicros(value: AnyRef): Long = value match {
    case ts: Timestamp => ts.toInstant.getEpochSecond * 1000000L + ts.getNanos / 1000L
    case instant: Instant => instant.getEpochSecond * 1000000L + instant.getNano / 1000L
    case ldt: LocalDateTime =>
      timestampMicros(Timestamp.valueOf(ldt))
    case odt: OffsetDateTime => timestampMicros(odt.toInstant)
    case zdt: ZonedDateTime => timestampMicros(zdt.toInstant)
    case n: Number => n.longValue()
    case s: String => timestampMicros(Timestamp.valueOf(s))
    case other =>
      throw new IllegalArgumentException(s"Unsupported timestamp value ${other.getClass}")
  }

  private def bytes(value: AnyRef): Array[Byte] = value match {
    case b: Array[Byte] => b
    case buffer: ByteBuffer =>
      val duplicate = buffer.duplicate()
      val result = new Array[Byte](duplicate.remaining())
      duplicate.get(result)
      result
    case text: Text => text.toString.getBytes(StandardCharsets.UTF_8)
    case other => other.toString.getBytes(StandardCharsets.UTF_8)
  }

  private def number(value: AnyRef): Number = value match {
    case n: Number => n
    case s: String => BigDecimal(s).bigDecimal
    case other => throw new IllegalArgumentException(s"Expected a number, got ${other.getClass}")
  }
}
