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

package org.apache.kyuubi.server

import org.apache.hive.service.rpc.thrift.{TBinaryColumn, TBoolColumn, TByteColumn, TDoubleColumn, TI16Column, TI32Column, TI64Column, TRowSet, TStringColumn}

import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.OperationHandle
import org.apache.kyuubi.service.BackendService

trait BackendServiceFetchRowsRate extends BackendService {
  abstract override def fetchResults(
      operationHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Int,
      fetchLog: Boolean): TRowSet = {
    val rowSet = super.fetchResults(operationHandle, orientation, maxRows, fetchLog)
    val rowsSize =
      if (rowSet.getColumnsSize > 0) {
        rowSet.getColumns.get(0).getFieldValue match {
          case t: TStringColumn => t.getValues.size()
          case t: TDoubleColumn => t.getValues.size()
          case t: TI64Column => t.getValues.size()
          case t: TI32Column => t.getValues.size()
          case t: TI16Column => t.getValues.size()
          case t: TBoolColumn => t.getValues.size()
          case t: TByteColumn => t.getValues.size()
          case t: TBinaryColumn => t.getValues.size()
          case _ => 0
        }
      } else {
        rowSet.getRowsSize
      }
    MetricsSystem.tracing(_.markMeter(
      if (fetchLog) MetricsConstants.FETCH_LOG_ROWS_RATE
      else MetricsConstants.FETCH_RESULT_ROWS_RATE,
      rowsSize))
    rowSet
  }
}
