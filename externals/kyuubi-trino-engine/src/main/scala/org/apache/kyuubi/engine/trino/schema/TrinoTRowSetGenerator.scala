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

package org.apache.kyuubi.engine.trino.schema

import io.trino.client.{ClientTypeSignature, Column}
import io.trino.client.ClientStandardTypes._

import org.apache.kyuubi.engine.schema.AbstractTRowSetGenerator
import org.apache.kyuubi.engine.trino.schema.RowSet.toHiveString
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId._
import org.apache.kyuubi.util.RowSetUtils._

class TrinoTRowSetGenerator
  extends AbstractTRowSetGenerator[Seq[Column], Seq[_], ClientTypeSignature] {

  override def getColumnSizeFromSchemaType(schema: Seq[Column]): Int = schema.length

  override def getColumnType(schema: Seq[Column], ordinal: Int): ClientTypeSignature = {
    schema(ordinal).getTypeSignature
  }

  override def isColumnNullAt(row: Seq[_], ordinal: Int): Boolean =
    row(ordinal) == null

  override def getColumnAs[T](row: Seq[_], ordinal: Int): T =
    row(ordinal).asInstanceOf[T]

  override def toTColumn(rows: Seq[Seq[_]], ordinal: Int, typ: ClientTypeSignature): TColumn = {
    val nulls = new java.util.BitSet()
    typ.getRawType match {
      case BOOLEAN => toTTypeColumn(BOOLEAN_TYPE, rows, ordinal)
      case TINYINT => toTTypeColumn(BINARY_TYPE, rows, ordinal)
      case SMALLINT => toTTypeColumn(TINYINT_TYPE, rows, ordinal)
      case INTEGER => toTTypeColumn(INT_TYPE, rows, ordinal)
      case BIGINT => toTTypeColumn(BIGINT_TYPE, rows, ordinal)
      case REAL => toTTypeColumn(FLOAT_TYPE, rows, ordinal)
      case DOUBLE => toTTypeColumn(DOUBLE_TYPE, rows, ordinal)
      case VARCHAR => toTTypeColumn(STRING_TYPE, rows, ordinal)
      case VARBINARY => toTTypeColumn(ARRAY_TYPE, rows, ordinal)
      case _ =>
        val rowSize = rows.length
        val values = new java.util.ArrayList[String](rowSize)
        var i = 0
        while (i < rowSize) {
          val row = rows(i)
          val isNull = isColumnNullAt(row, ordinal)
          nulls.set(i, isNull)
          val value = if (isNull) {
            ""
          } else {
            toHiveString(row(ordinal), typ)
          }
          values.add(value)
          i += 1
        }
        TColumn.stringVal(new TStringColumn(values, nulls))
    }
  }

  override def toTColumnValue(ordinal: Int, row: Seq[_], types: Seq[Column]): TColumnValue = {
    getColumnType(types, ordinal).getRawType match {
      case BOOLEAN => toTTypeColumnVal(BOOLEAN_TYPE, row, ordinal)
      case TINYINT => toTTypeColumnVal(BINARY_TYPE, row, ordinal)
      case SMALLINT => toTTypeColumnVal(TINYINT_TYPE, row, ordinal)
      case INTEGER => toTTypeColumnVal(INT_TYPE, row, ordinal)
      case BIGINT => toTTypeColumnVal(BIGINT_TYPE, row, ordinal)
      case REAL => toTTypeColumnVal(FLOAT_TYPE, row, ordinal)
      case DOUBLE => toTTypeColumnVal(DOUBLE_TYPE, row, ordinal)
      case VARCHAR => toTTypeColumnVal(STRING_TYPE, row, ordinal)
      case _ =>
        val tStrValue = new TStringValue
        if (row(ordinal) != null) {
          tStrValue.setValue(
            toHiveString(row(ordinal), types(ordinal).getTypeSignature))
        }
        TColumnValue.stringVal(tStrValue)
    }
  }

}
