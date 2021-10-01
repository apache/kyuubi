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

import org.apache.hive.jdbc.HiveDriver;
import org.apache.kyuubi.jdbc.hive.KyuubiConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Kyuubi JDBC driver to connect to Kyuubi server via HiveServer2 thrift protocol.
 */
public class KyuubiHiveDriver extends HiveDriver {
    static {
        try {
            DriverManager.registerDriver(new KyuubiHiveDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register driver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return acceptsURL(url) ? new KyuubiConnection(url, info) : null;
    }
}
