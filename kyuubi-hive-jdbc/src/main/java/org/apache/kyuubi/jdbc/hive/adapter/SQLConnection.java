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

package org.apache.kyuubi.jdbc.hive.adapter;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.Executor;

public interface SQLConnection extends Connection {

  @Override
  default CallableStatement prepareCall(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default String nativeSQL(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void commit() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void rollback() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setHoldability(int holdability) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int getHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Savepoint setSavepoint() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Savepoint setSavepoint(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void rollback(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Clob createClob() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Blob createBlob() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default NClob createNClob() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default SQLXML createSQLXML() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void abort(Executor executor) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int getNetworkTimeout() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }
}
