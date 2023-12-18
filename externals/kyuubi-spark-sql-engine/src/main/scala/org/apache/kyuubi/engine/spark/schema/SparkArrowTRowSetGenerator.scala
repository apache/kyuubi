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

package org.apache.kyuubi.engine.spark.schema

import java.nio.ByteBuffer

import org.apache.spark.sql.types._

import org.apache.kyuubi.engine.result.TRowSetGenerator
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

class SparkArrowTRowSetGenerator
  extends TRowSetGenerator[StructType, Array[Byte], DataType] {
  override def toColumnBasedSet(rows: Seq[Array[Byte]], schema: StructType): TRowSet = {
    require(schema.length == 1, "ArrowRowSetGenerator accepts only one single byte array")
    require(schema.head.dataType == BinaryType, "ArrowRowSetGenerator accepts only BinaryType")

    val tRowSet = new TRowSet(0, new java.util.ArrayList[TRow](1))
    val tColumn = toTColumn(rows, 1, schema.head.dataType)
    tRowSet.addToColumns(tColumn)
    tRowSet
  }

  override def toTColumn(rows: Seq[Array[Byte]], ordinal: Int, typ: DataType): TColumn = {
    require(rows.length == 1, "ArrowRowSetGenerator accepts only one single byte array")
    typ match {
      case BinaryType =>
        val values = new java.util.ArrayList[ByteBuffer](1)
        values.add(ByteBuffer.wrap(rows.head))
        TColumn.binaryVal(new TBinaryColumn(values, ByteBuffer.wrap(Array[Byte]())))
      case _ => throw new IllegalArgumentException(
          s"unsupported datatype $typ, ArrowRowSetGenerator accepts only BinaryType")
    }
  }

  override def toRowBasedSet(rows: Seq[Array[Byte]], schema: StructType): TRowSet = {
    throw new UnsupportedOperationException
  }

  override def getColumnSizeFromSchemaType(schema: StructType): Int = {
    throw new UnsupportedOperationException
  }

  override def getColumnType(schema: StructType, ordinal: Int): DataType = {
    throw new UnsupportedOperationException
  }

  override def isColumnNullAt(row: Array[Byte], ordinal: Int): Boolean = {
    throw new UnsupportedOperationException
  }

  override def getColumnAs[T](row: Array[Byte], ordinal: Int): T = {
    throw new UnsupportedOperationException
  }

  override def toTColumnValue(row: Array[Byte], ordinal: Int, types: StructType): TColumnValue = {
    throw new UnsupportedOperationException
  }

}
