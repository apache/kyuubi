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

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.{TColumn, TColumnDesc, TPrimitiveTypeEntry, TRow, TRowSet, TStringColumn, TTableSchema, TTypeDesc, TTypeEntry, TTypeId}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.OperationType.OperationType
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

class NoopOperation(typ: OperationType, session: Session, shouldFail: Boolean = false)
  extends AbstractOperation(typ, session) {
  override protected def runInternal(): Unit = {
    setState(OperationState.RUNNING)
    if (shouldFail) {
      val exception = KyuubiSQLException("noop operation err")
      setOperationException(exception)
      setState(OperationState.ERROR)
    }
    setHasResultSet(true)
  }

  override protected def beforeRun(): Unit = {
    setState(OperationState.PENDING)
  }

  override protected def afterRun(): Unit = {
    if (!OperationState.isTerminal(state)) {
      setState(OperationState.FINISHED)
    }
  }

  override def cancel(): Unit = {
    setState(OperationState.CANCELED)

  }

  override def close(): Unit = {
    setState(OperationState.CLOSED)
  }

  override def getResultSetSchema: TTableSchema = {
    val tColumnDesc = new TColumnDesc()
    tColumnDesc.setColumnName("noop")
    val desc = new TTypeDesc
    desc.addToTypes(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.STRING_TYPE)))
    tColumnDesc.setTypeDesc(desc)
    tColumnDesc.setComment("comment")
    tColumnDesc.setPosition(0)
    val schema = new TTableSchema()
    schema.addToColumns(tColumnDesc)
    schema
  }

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Int): TRowSet = {
    val col = TColumn.stringVal(new TStringColumn(Seq(typ.toString).asJava, ByteBuffer.allocate(0)))
    val tRowSet = new TRowSet(0, new java.util.ArrayList[TRow](0))
    tRowSet.addToColumns(col)
    tRowSet
  }

  override def shouldRunAsync: Boolean = false

  override def getOperationLog: Option[OperationLog] = None
}
