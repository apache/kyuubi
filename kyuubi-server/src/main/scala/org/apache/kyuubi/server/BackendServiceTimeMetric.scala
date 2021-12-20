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

import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.operation.{OperationHandle, OperationStatus}
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.service.BackendService
import org.apache.kyuubi.session.SessionHandle

trait BackendServiceTimeMetric extends BackendService {

  @throws[Exception]
  private def timeMetric[T](name: String)(f: => T): T = {
    val startTime = System.currentTimeMillis()
    try {
      f
    } finally {
      MetricsSystem.tracing(
        _.updateHistogram(name, System.currentTimeMillis() - startTime))
    }
  }

  abstract override def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddr: String,
      configs: Map[String, String]): SessionHandle = {
    timeMetric(MetricsConstants.OPEN_SESSION_MS) {
      super.openSession(protocol, user, password, ipAddr, configs)
    }
  }

  abstract override def closeSession(sessionHandle: SessionHandle): Unit = {
    timeMetric(MetricsConstants.CLOSE_SESSION_MS) {
      super.closeSession(sessionHandle)
    }
  }

  abstract override def getInfo(
      sessionHandle: SessionHandle,
      infoType: TGetInfoType): TGetInfoValue = {
    timeMetric(MetricsConstants.GET_INFO_MS) {
      super.getInfo(sessionHandle, infoType)
    }
  }

  abstract override def executeStatement(
      sessionHandle: SessionHandle,
      statement: String,
      runAsync: Boolean,
      queryTimeout: Long): OperationHandle = {
    timeMetric(MetricsConstants.EXECUTE_STATEMENT_MS) {
      super.executeStatement(sessionHandle, statement, runAsync, queryTimeout)
    }
  }

  abstract override def getTypeInfo(sessionHandle: SessionHandle): OperationHandle = {
    timeMetric(MetricsConstants.GET_TYPE_INFO_MS) {
      super.getTypeInfo(sessionHandle)
    }
  }

  abstract override def getCatalogs(sessionHandle: SessionHandle): OperationHandle = {
    timeMetric(MetricsConstants.GET_CATALOGS_MS) {
      super.getCatalogs(sessionHandle)
    }
  }

  abstract override def getSchemas(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String): OperationHandle = {
    timeMetric(MetricsConstants.GET_SCHEMAS_MS) {
      super.getSchemas(sessionHandle, catalogName, schemaName)
    }
  }

  abstract override def getTables(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: java.util.List[String]): OperationHandle = {
    timeMetric(MetricsConstants.GET_TABLES_MS) {
      super.getTables(sessionHandle, catalogName, schemaName, tableName, tableTypes)
    }
  }

  abstract override def getTableTypes(sessionHandle: SessionHandle): OperationHandle = {
    timeMetric(MetricsConstants.GET_TABLE_TYPES_MS) {
      super.getTableTypes(sessionHandle)
    }
  }

  abstract override def getColumns(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): OperationHandle = {
    timeMetric(MetricsConstants.GET_COLUMNS_MS) {
      super.getColumns(sessionHandle, catalogName, schemaName, tableName, columnName)
    }
  }

  abstract override def getFunctions(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      functionName: String): OperationHandle = {
    timeMetric(MetricsConstants.GET_FUNCTIONS_MS) {
      super.getFunctions(sessionHandle, catalogName, schemaName, functionName)
    }
  }

  abstract override def getOperationStatus(operationHandle: OperationHandle): OperationStatus = {
    timeMetric(MetricsConstants.GET_OPERATION_STATUS_MS) {
      super.getOperationStatus(operationHandle)
    }
  }

  abstract override def cancelOperation(operationHandle: OperationHandle): Unit = {
    timeMetric(MetricsConstants.CANCEL_OPERATION_MS) {
      super.cancelOperation(operationHandle)
    }
  }

  abstract override def closeOperation(operationHandle: OperationHandle): Unit = {
    timeMetric(MetricsConstants.CLOSE_OPERATION_MS) {
      super.closeOperation(operationHandle)
    }
  }

  abstract override def getResultSetMetadata(operationHandle: OperationHandle): TTableSchema = {
    timeMetric(MetricsConstants.GET_RESULT_SET_METADATA_MS) {
      super.getResultSetMetadata(operationHandle)
    }
  }

  abstract override def fetchResults(
      operationHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Int,
      fetchLog: Boolean): TRowSet = {
    timeMetric(MetricsConstants.FETCH_RESULTS_MS) {
      super.fetchResults(operationHandle, orientation, maxRows, fetchLog)
    }
  }

}
