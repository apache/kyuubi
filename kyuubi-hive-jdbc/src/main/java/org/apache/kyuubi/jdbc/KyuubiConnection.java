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

import java.lang.reflect.Field;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TSessionHandle;

public class KyuubiConnection extends HiveConnection {

    public KyuubiConnection(String url, Properties info) throws SQLException {
        super(url, info);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection is closed");
        }
        try {
            Field clientField = HiveConnection.class.getDeclaredField("client");
            clientField.setAccessible(true);
            TCLIService.Iface client = (TCLIService.Iface) clientField.get(this);
            Field handleField = HiveConnection.class.getDeclaredField("sessHandle");
            handleField.setAccessible(true);
            TSessionHandle sessionHandle = (TSessionHandle) handleField.get(this);
            return new KyuubiDatabaseMetaData(this, client, sessionHandle);
        } catch (NoSuchFieldException | IllegalAccessException rethrow) {
            throw new RuntimeException(rethrow);
        }
    }
}
