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

package org.apache.kyuubi.sql.plan.command

import org.apache.hive.service.rpc.thrift.{TProtocolVersion, TRowSet}

import org.apache.kyuubi.operation.FetchIterator
import org.apache.kyuubi.operation.FetchOrientation.{FETCH_FIRST, FETCH_NEXT, FETCH_PRIOR, FetchOrientation}
import org.apache.kyuubi.session.KyuubiSession
import org.apache.kyuubi.sql.plan.KyuubiTreeNode
import org.apache.kyuubi.sql.schema.{Row, RowSetHelper, Schema}

trait RunnableCommand extends KyuubiTreeNode {

  protected var iter: FetchIterator[Row] = _

  protected var isAdmin: Boolean = false

  def run(kyuubiSession: KyuubiSession): Unit

  def resultSchema: Schema

  def getNextRowSet(
      order: FetchOrientation,
      rowSetSize: Int,
      protocolVersion: TProtocolVersion): TRowSet = {
    order match {
      case FETCH_NEXT => iter.fetchNext()
      case FETCH_PRIOR => iter.fetchPrior(rowSetSize)
      case FETCH_FIRST => iter.fetchAbsolute(0)
    }
    val taken = iter.take(rowSetSize)
    val resultRowSet = RowSetHelper.toTRowSet(
      taken.toList.asInstanceOf[List[Row]],
      resultSchema,
      protocolVersion)
    resultRowSet.setStartRowOffset(iter.getPosition)
    resultRowSet
  }

  def setRole(isAdmin: Boolean): Unit = {
    this.isAdmin = isAdmin
  }
}
