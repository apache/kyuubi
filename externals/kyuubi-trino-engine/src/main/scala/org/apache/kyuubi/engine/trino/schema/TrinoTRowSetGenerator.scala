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

import org.apache.kyuubi.engine.result.TRowSetGenerator
import org.apache.kyuubi.engine.trino.schema.RowSet.toHiveString
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

class TrinoTRowSetGenerator
  extends TRowSetGenerator[Seq[Column], Seq[_], ClientTypeSignature] {

  override def getColumnSizeFromSchemaType(schema: Seq[Column]): Int = schema.length

  override def getColumnType(schema: Seq[Column], ordinal: Int): ClientTypeSignature =
    schema(ordinal).getTypeSignature

  override def isColumnNullAt(row: Seq[_], ordinal: Int): Boolean = row(ordinal) == null

  override def getColumnAs[T](row: Seq[_], ordinal: Int): T = row(ordinal).asInstanceOf[T]

  override def toTColumn(rows: Seq[Seq[_]], ordinal: Int, typ: ClientTypeSignature): TColumn = {
    typ.getRawType match {
      case BOOLEAN => asBooleanTColumn(rows, ordinal)
      case TINYINT => asByteTColumn(rows, ordinal)
      case SMALLINT => asShortTColumn(rows, ordinal)
      case INTEGER => asIntegerTColumn(rows, ordinal)
      case BIGINT => asLongTColumn(rows, ordinal)
      case REAL => asFloatTColumn(rows, ordinal)
      case DOUBLE => asDoubleTColumn(rows, ordinal)
      case VARCHAR => asStringTColumn(rows, ordinal)
      case VARBINARY => asByteArrayTColumn(rows, ordinal)
      case _ =>
        asStringTColumn(
          rows,
          ordinal,
          convertFunc = (row, ordinal) => toHiveString(getColumnAs[Any](row, ordinal), typ))
    }
  }

  override def toTColumnValue(row: Seq[_], ordinal: Int, types: Seq[Column]): TColumnValue = {
    getColumnType(types, ordinal).getRawType match {
      case BOOLEAN => asBooleanTColumnValue(row, ordinal)
      case TINYINT => asByteTColumnValue(row, ordinal)
      case SMALLINT => asShortTColumnValue(row, ordinal)
      case INTEGER => asIntegerTColumnValue(row, ordinal)
      case BIGINT => asLongTColumnValue(row, ordinal)
      case REAL => asFloatTColumnValue(row, ordinal)
      case DOUBLE => asDoubleTColumnValue(row, ordinal)
      case VARCHAR => asStringTColumnValue(row, ordinal)
      case _ =>
        asStringTColumnValue(
          row,
          ordinal,
          rawValue => toHiveString(rawValue, types(ordinal).getTypeSignature))
    }
  }

}
