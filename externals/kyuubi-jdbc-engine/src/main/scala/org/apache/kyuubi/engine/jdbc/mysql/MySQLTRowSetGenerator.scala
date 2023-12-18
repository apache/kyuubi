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
package org.apache.kyuubi.engine.jdbc.mysql

import java.sql.Types

import org.apache.kyuubi.engine.jdbc.schema.DefaultJdbcTRowSetGenerator
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TColumn, TColumnValue}

class MySQLTRowSetGenerator extends DefaultJdbcTRowSetGenerator {

  override def toTinyIntTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    toIntegerTColumn(rows, ordinal)

  override def toSmallIntTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    toIntegerTColumn(rows, ordinal)

  override def toTinyIntTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    toIntegerTColumnValue(row, ordinal)

  override def toSmallIntTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    toIntegerTColumnValue(row, ordinal)

  override def toIntegerTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn = {
    val colHead = if (rows.isEmpty) None else rows.head(ordinal)
    colHead match {
      case _: Integer => super.toIntegerTColumn(rows, ordinal)
      case _: java.lang.Long => super.toBigIntTColumn(rows, ordinal)
      case _ => super.toDefaultTColumn(rows, ordinal, Types.INTEGER)
    }
  }

  override protected def toIntegerTColumnValue(row: Seq[_], ordinal: Int): TColumnValue = {
    row(ordinal) match {
      case _: Integer => super.toIntegerTColumnValue(row, ordinal)
      case _: java.lang.Long => super.toBigIntTColumnValue(row, ordinal)
      case _ => super.toDefaultTColumnValue(row, ordinal, Types.INTEGER)
    }
  }

  override protected def toBigIntTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn = {
    val colHead = if (rows.isEmpty) None else rows.head(ordinal)
    colHead match {
      case v: java.lang.Long => super.toBigIntTColumn(rows, ordinal)
      case _ => super.toDefaultTColumn(rows, ordinal, Types.BIGINT)
    }
  }

  override protected def toBigIntTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    row(ordinal) match {
      case v: java.lang.Long => super.toBigIntTColumnValue(row, ordinal)
      case _ => super.toDefaultTColumnValue(row, ordinal, Types.BIGINT)
    }
}
