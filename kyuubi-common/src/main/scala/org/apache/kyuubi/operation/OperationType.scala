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

import org.apache.hive.service.rpc.thrift.TOperationType

object OperationType extends Enumeration {

  type OperationType = Value
  val UNKNOWN_OPERATION,
      EXECUTE_STATEMENT,
      GET_TYPE_INFO,
      GET_CATALOGS,
      GET_SCHEMAS,
      GET_TABLES,
      GET_TABLE_TYPES,
      GET_COLUMNS,
      GET_FUNCTIONS,
      INIT_ENGINE = Value

  def getOperationType(from: TOperationType): OperationType = {
    from match {
      case TOperationType.EXECUTE_STATEMENT => EXECUTE_STATEMENT
      case TOperationType.GET_TYPE_INFO => GET_TYPE_INFO
      case TOperationType.GET_CATALOGS => GET_CATALOGS
      case TOperationType.GET_SCHEMAS => GET_SCHEMAS
      case TOperationType.GET_TABLES => GET_TABLES
      case TOperationType.GET_TABLE_TYPES => GET_TABLE_TYPES
      case TOperationType.GET_COLUMNS => GET_COLUMNS
      case TOperationType.GET_FUNCTIONS => GET_FUNCTIONS
      case other =>
        throw new UnsupportedOperationException(s"Unsupported Operation type: ${other.toString}")
    }
  }

  def toTOperationType(from: OperationType): TOperationType = {
    from match {
      case EXECUTE_STATEMENT => TOperationType.EXECUTE_STATEMENT
      case GET_TYPE_INFO => TOperationType.GET_TYPE_INFO
      case GET_CATALOGS => TOperationType.GET_CATALOGS
      case GET_SCHEMAS => TOperationType.GET_SCHEMAS
      case GET_TABLES => TOperationType.GET_TABLES
      case GET_TABLE_TYPES => TOperationType.GET_TABLE_TYPES
      case GET_COLUMNS => TOperationType.GET_COLUMNS
      case GET_FUNCTIONS => TOperationType.GET_FUNCTIONS
      case INIT_ENGINE => TOperationType.UNKNOWN
      case other =>
        throw new UnsupportedOperationException(s"Unsupported Operation type: ${other.toString}")
    }
  }
}
