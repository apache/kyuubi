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

package yaooqinn.kyuubi.operation

import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.sql.types.StructType

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.cli.FetchOrientation
import yaooqinn.kyuubi.schema.RowSet
import yaooqinn.kyuubi.session.KyuubiSession

/**
 * Interface class of KyuubiOperation.
 */
trait IKyuubiOperation {

  /**
   * Get relative IKyuubiSession.
   */
  def getSession: KyuubiSession

  /**
   * Get relative OperationHandle.
   */
  def getHandle: OperationHandle

  /**
   * Get the protocol version of this IKyuubiOperation.
   */
  def getProtocolVersion: TProtocolVersion

  /**
   * Get current status of this IKyuubiOperation.
   */
  def getStatus: OperationStatus

  /**
   * Get operation log.
   */
  def getOperationLog: OperationLog

  /**
   * Run this IKyuubiOperation.
   * @throws KyuubiSQLException
   */
  @throws[KyuubiSQLException]
  def run(): Unit

  /**
   * Close this IKyuubiOperation.
   */
  def close(): Unit

  /**
   * Cancel this IKyuubiOperation.
   */
  def cancel(): Unit

  /**
   * Get the schema of operation result set.
   */
  def getResultSetSchema: StructType

  /**
   * Get the operation result set.
   * @param order the fetch orientation, FETCH_FIRST or FETCH_NEXT.
   * @param rowSetSize limit of result set.
   * @return
   */
  def getNextRowSet(order: FetchOrientation, rowSetSize: Long): RowSet

  /**
   * Check whether this IKyuubiOperation has run more than the configured timeout duration.
   */
  def isTimedOut: Boolean
}
