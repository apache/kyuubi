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

package org.apache.kyuubi.server.api.v1

import org.apache.kyuubi.session.SessionHandle

case class SessionOpenCount(openSessionCount: Int)

case class ExecPoolStatistic(execPoolSize: Int, execPoolActiveCount: Int)

case class SessionList(sessionList: Seq[SessionOverview])

case class SessionOverview(
  user: String,
  ipAddr: String,
  createTime: Long,
  sessionHandle: SessionHandle
)

case class InfoDetail(
  infoType: String,
  infoValue: String
)

case class SessionDetail(
  user: String,
  ipAddr: String,
  createTime: Long,
  sessionHandle: SessionHandle,
  lastAccessTime: Long,
  lastIdleTime: Long,
  noOperationTime: Long,
  configs: Map[String, String]
)

case class SessionOpenRequest(
  protocolVersion: Int,
  user: String,
  password: String,
  ipAddr: String,
  configs: Map[String, String]
)

case class StatementRequest(
  statement: String,
  runAsync: Boolean,
  queryTimeout: Long
)

case class GetSchemasRequest(
  catalogName: String,
  schemaName: String
)

case class GetTablesRequest(
  catalogName: String,
  schemaName: String,
  tableName: String,
  tableTypes: java.util.List[String]
)

case class GetColumnsRequest(
  catalogName: String,
  schemaName: String,
  tableName: String,
  columnName: String
)

case class GetFunctionsRequest(
  catalogName: String,
  schemaName: String,
  functionName: String
)
