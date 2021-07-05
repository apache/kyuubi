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

package org.apache.kyuubi.jdbc

import java.sql.{ResultSet, SQLException}

import scala.collection.JavaConverters._

import org.apache.hive.jdbc.{HiveDatabaseMetaData, HiveQueryResultSet}
import org.apache.hive.service.rpc.thrift.{TGetTablesReq, TSessionHandle, TStatusCode}
import org.apache.hive.service.rpc.thrift.TCLIService.Iface
import org.apache.thrift.TException

import org.apache.kyuubi.KyuubiSQLException

class KyuubiDatabaseMetaData(conn: KyuubiConnection, client: Iface, sessHandle: TSessionHandle)
  extends HiveDatabaseMetaData(conn, client, sessHandle) {
  override def getTables(
      catalog: String,
      schemaPattern: String,
      tableNamePattern: String,
      types: Array[String]): ResultSet = {
    val getTableReq: TGetTablesReq = new TGetTablesReq(sessHandle)
    getTableReq.setCatalogName(catalog)
    getTableReq.setSchemaName(if (schemaPattern == null) "%" else schemaPattern)
    getTableReq.setTableName(tableNamePattern)
    if (types != null) {
      getTableReq.setTableTypes(types.toList.asJava)
    }
    val getTableResp = try {
      client.GetTables(getTableReq)
    } catch {
      case e: TException => throw new SQLException(e.getMessage, "08S01", e)
    }
    val tStatus = getTableResp.getStatus
    if (tStatus.getStatusCode != TStatusCode.SUCCESS_STATUS) {
      throw KyuubiSQLException(tStatus)
    }
    new HiveQueryResultSet.Builder(conn)
      .setClient(client)
      .setSessionHandle(sessHandle)
      .setStmtHandle(getTableResp.getOperationHandle)
      .build
  }
}
