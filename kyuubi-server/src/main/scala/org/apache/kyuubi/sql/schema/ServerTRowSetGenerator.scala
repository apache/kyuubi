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

import org.apache.kyuubi.engine.result.TRowSetGenerator
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId._

class ServerTRowSetGenerator
  extends TRowSetGenerator[Schema, Row, TTypeId] {

  override def getColumnSizeFromSchemaType(schema: Schema): Int = schema.length

  override def getColumnType(schema: Schema, ordinal: Int): TTypeId = schema(ordinal).dataType

  override def isColumnNullAt(row: Row, ordinal: Int): Boolean = row.isNullAt(ordinal)

  override def getColumnAs[T](row: Row, ordinal: Int): T = row.getAs[T](ordinal)

  override def toTColumn(rows: Seq[Row], ordinal: Int, typ: TTypeId): TColumn = {
    typ match {
      case BOOLEAN_TYPE => asBooleanTColumn(rows, ordinal)
      case BINARY_TYPE => asShortTColumn(rows, ordinal)
      case TINYINT_TYPE => asShortTColumn(rows, ordinal)
      case INT_TYPE => asIntegerTColumn(rows, ordinal)
      case BIGINT_TYPE => asLongTColumn(rows, ordinal)
      case FLOAT_TYPE => asFloatTColumn(rows, ordinal)
      case DOUBLE_TYPE => asDoubleTColumn(rows, ordinal)
      case STRING_TYPE => asStringTColumn(rows, ordinal)
      case _ =>
        asStringTColumn(
          rows,
          ordinal,
          convertFunc = (row, ordinal) => (row.get(ordinal), typ).toString())
    }
  }

  override def toTColumnValue(row: Row, ordinal: Int, types: Schema): TColumnValue = {
    getColumnType(types, ordinal) match {
      case BOOLEAN_TYPE => asBooleanTColumnValue(row, ordinal)
      case BINARY_TYPE => asByteTColumnValue(row, ordinal)
      case TINYINT_TYPE => asShortTColumnValue(row, ordinal)
      case INT_TYPE => asIntegerTColumnValue(row, ordinal)
      case BIGINT_TYPE => asLongTColumnValue(row, ordinal)
      case FLOAT_TYPE => asFloatTColumnValue(row, ordinal)
      case DOUBLE_TYPE => asDoubleTColumnValue(row, ordinal)
      case STRING_TYPE => asStringTColumnValue(row, ordinal)
      case otherType =>
        asStringTColumnValue(row, ordinal, rawValue => (rawValue, otherType).toString())
    }
  }

}
