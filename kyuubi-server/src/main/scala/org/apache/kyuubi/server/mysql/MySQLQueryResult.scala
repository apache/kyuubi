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

package org.apache.kyuubi.server.mysql

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.server.mysql.constant.MySQLDataType

object MySQLQueryResult {

  def apply(schema: Seq[MySQLField], rows: Seq[Seq[Any]]): MySQLSimpleQueryResult = {
    new MySQLSimpleQueryResult(schema, rows)
  }

  def apply(schema: TTableSchema, rows: TRowSet): MySQLThriftQueryResult = {
    new MySQLThriftQueryResult(schema, rows)
  }
}

trait MySQLQueryResult {

  def colCount: Int

  def rowCount: Int

  def toColDefinePackets: Seq[MySQLPacket]

  def toRowPackets: Seq[MySQLPacket]

  def toPackets: Seq[MySQLPacket] = {
    val buf = Seq.newBuilder[MySQLPacket]
    buf +=
      MySQLFieldCountPacket(1, colCount) ++=
      toColDefinePackets +=
      MySQLEofPacket(colCount + 2) ++=
      toRowPackets +=
      MySQLEofPacket(colCount + rowCount + 3)
    buf.result
  }
}

class MySQLSimpleQueryResult(
  schema: Seq[MySQLField],
  rows: Seq[Seq[Any]]
) extends MySQLQueryResult {

  override def colCount: Int = schema.size

  override def rowCount: Int = rows.size

  override def toColDefinePackets: Seq[MySQLPacket] =
    schema.zipWithIndex.map { case (field, i) =>
      val sequenceId = 2 + i
      val decimals = 0 // TODO
      MySQLColumnDefinition41Packet(
        sequenceId = sequenceId,
        flags = 0,
        name = field.name,
        columnLength = 100,
        columnType = field.dataType,
        decimals = decimals
      )
    }

  override def toRowPackets: Seq[MySQLPacket] =
    rows.zipWithIndex.map { case (row, i) =>
      val sequenceId = colCount + 3 + i
      MySQLTextResultSetRowPacket(sequenceId = sequenceId, row = row)
    }
}

class MySQLThriftQueryResult(
  schema: TTableSchema,
  rows: TRowSet
) extends MySQLQueryResult {

  override def colCount: Int = schema.getColumnsSize

  override def rowCount: Int = rows.getRows.size

  override def toColDefinePackets: Seq[MySQLPacket] = schema.getColumns.asScala
    .zipWithIndex.map { case (tCol, i) => tColDescToMySQL(tCol, 2 + i) }

  override def toRowPackets: Seq[MySQLPacket] = rows.getRows.asScala
    .zipWithIndex.map { case (tRow, i) => tRowToMySQL(tRow, colCount + 3 + i) }

  private def tColDescToMySQL(
    tCol: TColumnDesc,
    sequenceId: Int
  ): MySQLColumnDefinition41Packet = {
    val tType = tCol.getTypeDesc
    val dataType = tTypeDescToMySQL(tType)
    val decimals = 0 // TODO
    MySQLColumnDefinition41Packet(
      sequenceId = sequenceId,
      flags = 0,
      name = tCol.getColumnName,
      columnLength = 100,
      columnType = dataType,
      decimals = decimals
    )
  }

  private def tRowToMySQL(tRow: TRow, sequenceId: Int): MySQLTextResultSetRowPacket = {
    val mysqlRow = tRow.getColVals.asScala.map {
      case tVal: TColumnValue if tVal.isSetBoolVal => tVal.getBoolVal.isValue
      case tVal: TColumnValue if tVal.isSetByteVal => tVal.getByteVal.getValue
      case tVal: TColumnValue if tVal.isSetI16Val => tVal.getI16Val.getValue
      case tVal: TColumnValue if tVal.isSetI32Val => tVal.getI32Val.getValue
      case tVal: TColumnValue if tVal.isSetI64Val => tVal.getI64Val.getValue
      case tVal: TColumnValue if tVal.isSetDoubleVal => tVal.getDoubleVal.getValue
      case tVal: TColumnValue if tVal.isSetStringVal => tVal.getStringVal.getValue
    }
    MySQLTextResultSetRowPacket(sequenceId = sequenceId, row = mysqlRow)
  }

  private def tTypeDescToMySQL(typeDesc: TTypeDesc): MySQLDataType =
    typeDesc.getTypes.asScala.toList match {
      case entry :: Nil if entry.isSetPrimitiveEntry =>
        MySQLDataType.ofThriftType(entry.getPrimitiveEntry.getType)
      case _ =>
        // MySQL does not support nest data types
        MySQLDataType.VAR_STRING
    }
}
