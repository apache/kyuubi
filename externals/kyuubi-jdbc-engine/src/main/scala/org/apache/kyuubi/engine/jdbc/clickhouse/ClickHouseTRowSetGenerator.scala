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
package org.apache.kyuubi.engine.jdbc.clickhouse

import java.lang.{Long => JLong, Short => JShort}
import java.sql.Types.{ARRAY, OTHER}

import org.apache.kyuubi.engine.jdbc.schema.DefaultJdbcTRowSetGenerator
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TColumn, TColumnValue}

class ClickHouseTRowSetGenerator extends DefaultJdbcTRowSetGenerator {
  override def toTinyIntTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn = {
    super.asByteTColumn(rows, ordinal)
  }

  override def toSmallIntTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn = {
    val colHead = if (rows.isEmpty) None else rows.head(ordinal)
    colHead match {
      case _: JShort => super.toSmallIntTColumn(rows, ordinal)
      case _ => super.asShortTColumn(
          rows,
          ordinal,
          convertFunc = (row, ordinal) => JShort.valueOf(row(ordinal).toString))
    }
  }

  override def toIntegerTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn = {
    val colHead = if (rows.isEmpty) None else rows.head(ordinal)
    colHead match {
      case _: Integer => super.toIntegerTColumn(rows, ordinal)
      case _ => super.asIntegerTColumn(
          rows,
          ordinal,
          convertFunc = (row, ordinal) => Integer.valueOf(row(ordinal).toString))
    }
  }

  override def toBigIntTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn = {
    val colHead = if (rows.isEmpty) None else rows.head(ordinal)
    colHead match {
      case _: JLong => super.toBigIntTColumn(rows, ordinal)
      case _ => super.asLongTColumn(
          rows,
          ordinal,
          convertFunc = (row, ordinal) => JLong.valueOf(row(ordinal).toString))
    }
  }

  override def toVarcharTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn = {
    val colHead = if (rows.isEmpty) None else rows.head(ordinal)
    colHead match {
      case _: String => super.toVarcharTColumn(rows, ordinal)
      case _ =>
        asStringTColumn(rows, ordinal, convertFunc = (row, ordinal) => String.valueOf(row(ordinal)))
    }
  }

  override def toTinyIntTColumnValue(row: Seq[_], ordinal: Int): TColumnValue = {
    super.asByteTColumnValue(row, ordinal)
  }

  override def toSmallIntTColumnValue(row: Seq[_], ordinal: Int): TColumnValue = {
    asShortTColumnValue(row, ordinal, rawValue => JShort.valueOf(rawValue.toString))
  }

  override def toIntegerTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    asIntegerTColumnValue(row, ordinal, rawValue => Integer.valueOf(rawValue.toString))

  override def toBigIntTColumnValue(row: Seq[_], ordinal: Int): TColumnValue = {
    asLongTColumnValue(row, ordinal, rawValue => JLong.valueOf(rawValue.toString))
  }

  override def toHiveString(data: Any, sqlType: Int): String =
    (data, sqlType) match {
      case (array: Array[_], ARRAY) => arrayToString(array)
      case (array: Array[_], OTHER) => arrayToString(array)
      case (other, _) => super.toHiveString(other, sqlType)
    }

  private def arrayToString(arr: Any): String = {
    arr match {
      case a: Array[_] => "[" + a.map(arrayToString).mkString(", ") + "]"
      case x => x.toString
    }
  }
}
