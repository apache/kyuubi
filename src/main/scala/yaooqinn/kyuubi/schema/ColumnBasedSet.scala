/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.schema

import java.nio.ByteBuffer
import java.util.BitSet

import scala.collection.JavaConverters._

import org.apache.hive.service.cli.thrift._
import org.apache.spark.sql.{Row, SparkSQLUtils}
import org.apache.spark.sql.types.{BinaryType, _}

case class ColumnBasedSet(types: StructType, rows: Seq[Row]) extends RowSet {
  import ColumnBasedSet._

  override def toTRowSet: TRowSet = {
    val tRowSet = new TRowSet(0, Seq[TRow]().asJava)
    (0 until types.length).map(i => toTColumn(i, types(i).dataType)).foreach(tRowSet.addToColumns)
    tRowSet
  }

  private[this] def toTColumn(ordinal: Int, typ: DataType): TColumn = {
    val nulls = new BitSet()
    typ match {
      case BooleanType =>
        val values = rows.zipWithIndex.map { case (row, i) =>
            nulls.set(i, row.isNullAt(ordinal))
            if (row.isNullAt(ordinal)) true else row.getBoolean(ordinal)
        }.map(_.asInstanceOf[java.lang.Boolean]).asJava
        TColumn.boolVal(new TBoolColumn(values, bitSetToBuffer(nulls)))
      case ByteType =>
        val values = rows.zipWithIndex.map { case (row, i) =>
          nulls.set(i, row.isNullAt(ordinal))
          if (row.isNullAt(ordinal)) 0 else row.getByte(ordinal)
        }.map(_.asInstanceOf[java.lang.Byte]).asJava
        TColumn.byteVal(new TByteColumn(values, bitSetToBuffer(nulls)))
      case ShortType =>
        val values = rows.zipWithIndex.map { case (row, i) =>
          nulls.set(i, row.isNullAt(ordinal))
          if (row.isNullAt(ordinal)) 0 else row.getShort(ordinal)
        }.map(_.asInstanceOf[java.lang.Short]).asJava
        TColumn.i16Val(new TI16Column(values, bitSetToBuffer(nulls)))
      case IntegerType =>
        val values = rows.zipWithIndex.map { case (row, i) =>
          nulls.set(i, row.isNullAt(ordinal))
          if (row.isNullAt(ordinal)) 0 else row.getInt(ordinal)
        }.map(_.asInstanceOf[java.lang.Integer]).asJava
        TColumn.i32Val(new TI32Column(values, bitSetToBuffer(nulls)))
      case LongType =>
        val values = rows.zipWithIndex.map { case (row, i) =>
          nulls.set(i, row.isNullAt(ordinal))
          if (row.isNullAt(ordinal)) 0 else row.getLong(ordinal)
        }.map(_.asInstanceOf[java.lang.Long]).asJava
        TColumn.i64Val(new TI64Column(values, bitSetToBuffer(nulls)))
      case FloatType =>
        val values = rows.zipWithIndex.map { case (row, i) =>
          nulls.set(i, row.isNullAt(ordinal))
          if (row.isNullAt(ordinal)) 0 else row.getFloat(ordinal)
        }.map(_.asInstanceOf[java.lang.Double]).asJava
        TColumn.doubleVal(new TDoubleColumn(values, bitSetToBuffer(nulls)))
      case DoubleType =>
        val values = rows.zipWithIndex.map { case (row, i) =>
          nulls.set(i, row.isNullAt(ordinal))
          if (row.isNullAt(ordinal)) 0 else row.getDouble(ordinal)
        }.map(_.asInstanceOf[java.lang.Double]).asJava
        TColumn.doubleVal(new TDoubleColumn(values, bitSetToBuffer(nulls)))
      case StringType =>
        val values = rows.zipWithIndex.map { case (row, i) =>
          nulls.set(i, row.isNullAt(ordinal))
          if (row.isNullAt(ordinal)) EMPTY_STRING else row.getString(ordinal)
        }.asJava
        TColumn.stringVal(new TStringColumn(values, bitSetToBuffer(nulls)))
      case BinaryType =>
        val values = rows.zipWithIndex.map { case (row, i) =>
          nulls.set(i, row.isNullAt(ordinal))
          if (row.isNullAt(ordinal)) {
            EMPTY_BINARY
          } else {
            ByteBuffer.wrap(row.getAs[Array[Byte]](ordinal))
          }
        }.asJava
        TColumn.binaryVal(new TBinaryColumn(values, bitSetToBuffer(nulls)))
      case _ =>
        val values = rows.zipWithIndex.map { case (row, i) =>
          nulls.set(i, row.isNullAt(ordinal))
          if (row.isNullAt(ordinal)) {
            EMPTY_STRING
          } else {
            SparkSQLUtils.toHiveString((row.get(ordinal), typ))
          }
        }.asJava
        TColumn.stringVal(new TStringColumn(values, bitSetToBuffer(nulls)))
    }
  }

}

object ColumnBasedSet {
  private val EMPTY_STRING = ""
  private val EMPTY_BINARY = ByteBuffer.allocate(0)
  private val MASKS = Array[Byte](0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80.toByte)

  private def bitSetToBuffer(bitset: BitSet): ByteBuffer = {
    val nulls = new Array[Byte](1 + (bitset.length / 8))
    (0 until bitset.length).foreach { i =>
      nulls(i / 8) = (nulls(i / 8) | (if (bitset.get(i)) MASKS(i % 8) else 0.toByte)).toByte
    }
    ByteBuffer.wrap(nulls)
  }
}
