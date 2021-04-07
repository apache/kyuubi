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

import java.util.concurrent.Future

import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.shade.hive.service.rpc.thrift.{TProtocolVersion, TRowSet, TTableSchema}

trait Operation {

  def run(): Unit
  def cancel(): Unit
  def close(): Unit

  def getProtocolVersion: TProtocolVersion
  def getResultSetSchema: TTableSchema
  def getNextRowSet(order: FetchOrientation, rowSetSize: Int): TRowSet

  def getSession: Session
  def getHandle: OperationHandle
  def getStatus: OperationStatus
  def getOperationLog: Option[OperationLog]

  def getBackgroundHandle: Future[_]
  def shouldRunAsync: Boolean
  def isTimedOut: Boolean

}

object Operation {
  val DEFAULT_FETCH_ORIENTATION_SET: Set[FetchOrientation] =
    Set(FetchOrientation.FETCH_NEXT, FetchOrientation.FETCH_FIRST, FetchOrientation.FETCH_PRIOR)
}
