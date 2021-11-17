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

package org.apache.hive.beeline;

import org.apache.kyuubi.jdbc.hive.KyuubiConnection;
import org.apache.kyuubi.jdbc.hive.logs.InPlaceUpdateStream;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class KyuubiDatabaseConnection extends DatabaseConnection {
  private KyuubiBeeLine beeLine;
  KyuubiDatabaseConnection(KyuubiBeeLine beeLine, String driver, String url,
                           Properties properties) throws SQLException {
    super(beeLine, driver, url, properties);
    this.beeLine = beeLine;
  }

  @Override
  void setConnection(Connection connection) {
    if (connection != null) {
      beeLine.debug("The connection to set is kind of " + connection.getClass().getSimpleName());
      if (connection instanceof KyuubiConnection) {
       KyuubiConnection kyuubiConnection = (KyuubiConnection) connection;
        InPlaceUpdateStream.EventNotifier eventNotifier = new InPlaceUpdateStream.EventNotifier();
        Thread logThread = new Thread(beeLine.commands.createConnectionLogRunnable(connection,
          eventNotifier));
        logThread.setDaemon(true);
        logThread.start();

      }

    }

    super.setConnection(connection);
  }
}
