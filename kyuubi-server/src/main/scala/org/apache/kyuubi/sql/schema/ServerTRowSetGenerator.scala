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

package org.apache.kyuubi.sql.schema

import org.apache.kyuubi.engine.schema.AbstractTRowSetGenerator
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId._
import org.apache.kyuubi.util.RowSetUtils._

class ServerTRowSetGenerator
  extends AbstractTRowSetGenerator[Schema, Row, TTypeId] {

  override def getColumnSizeFromSchemaType(schema: Schema): Int = schema.length

  override def getColumnType(schema: Schema, ordinal: Int): TTypeId = schema(ordinal).dataType

  override def isColumnNullAt(row: Row, ordinal: Int): Boolean = row.isNullAt(ordinal)

  override def getColumnAs[T](row: Row, ordinal: Int): T = row.getAs[T](ordinal)

  override def toTColumn(rows: Seq[Row], ordinal: Int, typ: TTypeId): TColumn = {
    val nulls = new java.util.BitSet()
    typ match {
      case t @ (BOOLEAN_TYPE | BINARY_TYPE | BINARY_TYPE | TINYINT_TYPE | INT_TYPE |
          BIGINT_TYPE | FLOAT_TYPE | DOUBLE_TYPE | STRING_TYPE) =>
        toTTypeColumn(t, rows, ordinal)

      case _ =>
        var i = 0
        val rowSize = rows.length
        val values = new java.util.ArrayList[String](rowSize)
        while (i < rowSize) {
          val row = rows(i)
          val isNull = isColumnNullAt(row, ordinal)
          nulls.set(i, isNull)
          val value = if (isNull) {
            ""
          } else {
            (row.get(ordinal), typ).toString()
          }
          values.add(value)
          i += 1
        }
        TColumn.stringVal(new TStringColumn(values, nulls))
    }
  }

  override def toTColumnValue(ordinal: Int, row: Row, types: Schema): TColumnValue = {
    getColumnType(types, ordinal) match {
      case t @ (BOOLEAN_TYPE | BINARY_TYPE | BINARY_TYPE | TINYINT_TYPE | INT_TYPE |
          BIGINT_TYPE | FLOAT_TYPE | DOUBLE_TYPE | STRING_TYPE) =>
        toTTypeColumnVal(t, row, ordinal)

      case _ =>
        val tStrValue = new TStringValue
        if (!isColumnNullAt(row, ordinal)) {
          tStrValue.setValue((row.get(ordinal), types(ordinal).dataType).toString())
        }
        TColumnValue.stringVal(tStrValue)
    }
  }

}
