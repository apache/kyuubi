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

package org.apache.kyuubi.jdbc.hive;

import static org.apache.kyuubi.jdbc.hive.JdbcConnectionParams.AUTH_PASSWD;
import static org.apache.kyuubi.jdbc.hive.JdbcConnectionParams.AUTH_USER;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.kyuubi.jdbc.hive.adapter.SQLDataSource;

/** KyuubiDataSource. */
public class KyuubiDataSource implements SQLDataSource {

  public KyuubiDataSource() {}

  @Override
  public Connection getConnection() throws SQLException {
    return getConnection(null, null);
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    try {
      Properties info = new Properties();
      if (username != null) {
        info.setProperty(AUTH_USER, username);
      }
      if (password != null) {
        info.setProperty(AUTH_PASSWD, password);
      }
      return new KyuubiConnection("", info);
    } catch (Exception ex) {
      throw new KyuubiSQLException("Error in getting HiveConnection", ex);
    }
  }
}
