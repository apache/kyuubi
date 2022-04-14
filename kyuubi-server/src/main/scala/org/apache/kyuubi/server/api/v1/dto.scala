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

class VersionInfo(val version: String = org.apache.kyuubi.KYUUBI_VERSION)

case class SessionOpenCount(openSessionCount: Int)

case class ExecPoolStatistic(execPoolSize: Int, execPoolActiveCount: Int)

case class SessionData(
    sessionHandle: SessionHandle,
    user: String,
    ipAddr: String,
    conf: Map[String, String],
    createTime: Long,
    duration: Long,
    idleTime: Long)

case class InfoDetail(
    infoType: String,
    infoValue: String)

case class SessionOpenRequest(
    protocolVersion: Int,
    user: String,
    password: String,
    ipAddr: String,
    configs: Map[String, String])

case class StatementRequest(
    statement: String,
    runAsync: Boolean,
    queryTimeout: Long)

case class GetSchemasRequest(
    catalogName: String,
    schemaName: String)

case class GetTablesRequest(
    catalogName: String,
    schemaName: String,
    tableName: String,
    tableTypes: java.util.List[String])

case class GetColumnsRequest(
    catalogName: String,
    schemaName: String,
    tableName: String,
    columnName: String)

case class GetFunctionsRequest(
    catalogName: String,
    schemaName: String,
    functionName: String)

case class GetPrimaryKeysRequest(
    catalogName: String,
    schemaName: String,
    tableName: String)

case class GetCrossReferenceRequest(
    primaryCatalog: String,
    primarySchema: String,
    primaryTable: String,
    foreignCatalog: String,
    foreignSchema: String,
    foreignTable: String)

case class OpActionRequest(action: String)

case class ResultSetMetaData(columns: Seq[ColumnDesc])

case class ColumnDesc(
    columnName: String,
    dataType: String,
    columnIndex: Int,
    precision: Int,
    scale: Int,
    comment: String)

case class OperationLog(logRowSet: Seq[String], rowCount: Int)

case class ResultRowSet(rows: Seq[Row], rowCount: Int)

case class Row(fields: Seq[Field])

case class Field(dataType: String, value: Any)

/**
 * The request body for batch job submission.
 *
 * @param batchType the batch job type, such as spark, flink, etc.
 * @param resource the main resource jar, required.
 * @param proxyUser the proxy user, optional.
 * @param className the main class name, required.
 * @param name a name of your batch job, optional.
 * @param conf arbitrary configuration properties, optional.
 * @param args comma-separated list of batch job arguments, optional.
 */
case class BatchRequest(
    batchType: String,
    resource: String,
    proxyUser: String,
    className: String,
    name: String,
    conf: Map[String, String],
    args: java.util.List[String])
