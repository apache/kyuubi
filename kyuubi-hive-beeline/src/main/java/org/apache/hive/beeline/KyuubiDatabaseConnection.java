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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import org.apache.kyuubi.jdbc.hive.KyuubiConnection;
import org.apache.kyuubi.jdbc.hive.logs.InPlaceUpdateStream;

public class KyuubiDatabaseConnection extends DatabaseConnection {
  private static final String HIVE_VAR_PREFIX = "hivevar:";
  private static final String HIVE_CONF_PREFIX = "hiveconf:";

  private KyuubiBeeLine beeLine;
  private String driver;
  private String url;
  private Properties info;

  KyuubiDatabaseConnection(KyuubiBeeLine beeLine, String driver, String url, Properties info)
      throws SQLException {
    super(beeLine, driver, url, info);
    this.beeLine = beeLine;
    this.driver = driver;
    this.url = url;
    this.info = info;
  }

  @Override
  boolean connect() throws SQLException {
    try {
      if (driver != null && driver.length() != 0) {
        Class.forName(driver);
      }
    } catch (ClassNotFoundException cnfe) {
      return beeLine.error(cnfe);
    }

    boolean isDriverRegistered = false;
    try {
      isDriverRegistered = DriverManager.getDriver(getUrl()) != null;
    } catch (Exception e) {
    }

    try {
      close();
    } catch (Exception e) {
      return beeLine.error(e);
    }

    Map<String, String> hiveVars = beeLine.getOpts().getHiveVariables();
    if (hiveVars != null) {
      for (Map.Entry<String, String> var : hiveVars.entrySet()) {
        info.put(HIVE_VAR_PREFIX + var.getKey(), var.getValue());
      }
    }

    Map<String, String> hiveConfVars = beeLine.getOpts().getHiveConfVariables();
    if (hiveConfVars != null) {
      for (Map.Entry<String, String> var : hiveConfVars.entrySet()) {
        info.put(HIVE_CONF_PREFIX + var.getKey(), var.getValue());
      }
    }

    if (isDriverRegistered) {
      boolean useDefaultDriver =
          beeLine.getDefaultDriver() != null && beeLine.getDefaultDriver().acceptsURL(url);
      if (driver != null && !driver.equals(KyuubiBeeLine.KYUUBI_BEELINE_DEFAULT_JDBC_DRIVER)) {
        beeLine.debug("Use default kyuubi driver and specified driver is:" + driver);
      }

      if (useDefaultDriver) {
        beeLine.debug("Use the default driver:" + KyuubiBeeLine.KYUUBI_BEELINE_DEFAULT_JDBC_DRIVER);
        setConnection(getConnectionFromDefaultDriver(getUrl(), info));
      } else {
        beeLine.debug("Not use the default kyuubi driver and specified driver is:" + driver);
        // if the driver registered in the driver manager, get the connection via the driver manager
        setConnection(DriverManager.getConnection(getUrl(), info));
      }
    } else {
      beeLine.debug("Use the driver from local added jar file.");
      setConnection(getConnectionFromLocalDriver(getUrl(), info));
    }
    setDatabaseMetaData(getConnection().getMetaData());

    try {
      beeLine.info(
          beeLine.loc(
              "connected",
              new Object[] {
                getDatabaseMetaData().getDatabaseProductName(),
                getDatabaseMetaData().getDatabaseProductVersion()
              }));
    } catch (Exception e) {
      beeLine.handleException(e);
    }

    try {
      beeLine.info(
          beeLine.loc(
              "driver",
              new Object[] {
                getDatabaseMetaData().getDriverName(), getDatabaseMetaData().getDriverVersion()
              }));
    } catch (Exception e) {
      beeLine.handleException(e);
    }

    return true;
  }

  public Connection getConnectionFromDefaultDriver(String url, Properties properties)
      throws SQLException {
    properties.setProperty(KyuubiConnection.BEELINE_MODE_PROPERTY, Boolean.toString(true));
    KyuubiConnection kyuubiConnection =
        (KyuubiConnection) beeLine.getDefaultDriver().connect(url, properties);

    InPlaceUpdateStream.EventNotifier eventNotifier = new InPlaceUpdateStream.EventNotifier();
    /** there is nothing we want to request in the first place. */
    eventNotifier.progressBarCompleted();

    Thread logThread =
        new Thread(beeLine.getCommands().createLogRunnable(kyuubiConnection, eventNotifier));
    logThread.setDaemon(true);
    logThread.start();
    kyuubiConnection.setEngineLogThread(logThread);

    kyuubiConnection.waitLaunchEngineToComplete();
    logThread.interrupt();
    kyuubiConnection.executeInitSql();

    return kyuubiConnection;
  }
}
