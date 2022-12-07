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

package org.apache.kyuubi.operation

import java.nio.ByteBuffer
import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.{TColumn, TColumnDesc, TGetResultSetMetadataResp, TPrimitiveTypeEntry, TRow, TRowSet, TStringColumn, TTableSchema, TTypeDesc, TTypeEntry, TTypeId}

import org.apache.kyuubi.engine.ApplicationInfo
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.util.ThriftUtils

abstract class KyuubiApplicationOperation(session: Session) extends KyuubiOperation(session) {

  private[kyuubi] def currentApplicationInfo: Option[ApplicationInfo]

  override def getResultSetMetadata: TGetResultSetMetadataResp = {
    val schema = new TTableSchema()
    Seq("key", "value").zipWithIndex.foreach { case (colName, position) =>
      val tColumnDesc = new TColumnDesc()
      tColumnDesc.setColumnName(colName)
      val tTypeDesc = new TTypeDesc()
      tTypeDesc.addToTypes(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.STRING_TYPE)))
      tColumnDesc.setTypeDesc(tTypeDesc)
      tColumnDesc.setPosition(position)
      schema.addToColumns(tColumnDesc)
    }
    val resp = new TGetResultSetMetadataResp
    resp.setSchema(schema)
    resp.setStatus(okStatusWithHints(Seq.empty))
    resp
  }

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Int): TRowSet = {
    currentApplicationInfo.map(_.toMap).map { state =>
      val tRow = new TRowSet(0, new JArrayList[TRow](state.size))
      Seq(state.keys, state.values.map(Option(_).getOrElse(""))).map(_.toSeq.asJava).foreach {
        col =>
          val tCol = TColumn.stringVal(new TStringColumn(col, ByteBuffer.allocate(0)))
          tRow.addToColumns(tCol)
      }
      tRow
    }.getOrElse(ThriftUtils.EMPTY_ROW_SET)
  }
}
