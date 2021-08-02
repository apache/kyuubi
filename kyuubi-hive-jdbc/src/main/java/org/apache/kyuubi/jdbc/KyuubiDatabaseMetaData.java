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

package org.apache.kyuubi.jdbc;

import org.apache.hive.jdbc.HiveDatabaseMetaData;
import org.apache.hive.jdbc.HiveQueryResultSet;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

public class KyuubiDatabaseMetaData extends HiveDatabaseMetaData {
    private final KyuubiConnection conn;
    private final TCLIService.Iface client;
    private final TSessionHandle sessHandle;

    public KyuubiDatabaseMetaData(KyuubiConnection conn, TCLIService.Iface client, TSessionHandle sessHandle) {
        super(conn, client, sessHandle);
        this.conn = conn;
        this.client = client;
        this.sessHandle = sessHandle;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern,
                               String tableNamePattern, String[] types) throws SQLException {

        TGetTablesReq getTableReq = new TGetTablesReq(sessHandle);
        getTableReq.setCatalogName(catalog);
        getTableReq.setSchemaName(schemaPattern == null ? "%" : schemaPattern);
        getTableReq.setTableName(tableNamePattern);
        if (types != null) {
            getTableReq.setTableTypes(Arrays.asList(types));
        }
        TGetTablesResp getTableResp;
        try {
            getTableResp = client.GetTables(getTableReq);
        } catch (TException rethrow) {
            throw new SQLException(rethrow.getMessage(), "08S01", rethrow);
        }
        TStatus tStatus = getTableResp.getStatus();
        if (tStatus.getStatusCode() != TStatusCode.SUCCESS_STATUS) {
            throw new HiveSQLException(tStatus);
        }
        new HiveQueryResultSet.Builder(conn)
                .setClient(client)
                .setSessionHandle(sessHandle)
                .setStmtHandle(getTableResp.getOperationHandle())
                .build();
        return super.getTables(catalog, schemaPattern, tableNamePattern, types);
    }
}
