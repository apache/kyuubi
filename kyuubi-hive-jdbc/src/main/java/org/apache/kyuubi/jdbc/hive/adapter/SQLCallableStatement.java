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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

@SuppressWarnings("deprecation")
public interface SQLCallableStatement extends CallableStatement {

  @Override
  default void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void registerOutParameter(int parameterIndex, int sqlType, int scale)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean wasNull() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default String getString(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean getBoolean(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default byte getByte(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default short getShort(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int getInt(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default long getLong(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default float getFloat(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default double getDouble(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default byte[] getBytes(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Date getDate(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Time getTime(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Timestamp getTimestamp(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Object getObject(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Ref getRef(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Blob getBlob(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Clob getClob(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Array getArray(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Date getDate(int parameterIndex, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Time getTime(int parameterIndex, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void registerOutParameter(int parameterIndex, int sqlType, String typeName)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void registerOutParameter(String parameterName, int sqlType) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void registerOutParameter(String parameterName, int sqlType, int scale)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void registerOutParameter(String parameterName, int sqlType, String typeName)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default URL getURL(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setURL(String parameterName, URL val) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setNull(String parameterName, int sqlType) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setBoolean(String parameterName, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setByte(String parameterName, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setShort(String parameterName, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setInt(String parameterName, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setLong(String parameterName, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setFloat(String parameterName, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setDouble(String parameterName, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setString(String parameterName, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setBytes(String parameterName, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setDate(String parameterName, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setTime(String parameterName, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setTimestamp(String parameterName, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setBinaryStream(String parameterName, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setObject(String parameterName, Object x, int targetSqlType, int scale)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setObject(String parameterName, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setCharacterStream(String parameterName, Reader reader, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default String getString(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean getBoolean(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default byte getByte(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default short getShort(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int getInt(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default long getLong(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default float getFloat(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default double getDouble(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default byte[] getBytes(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Date getDate(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Time getTime(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Timestamp getTimestamp(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Object getObject(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default BigDecimal getBigDecimal(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Ref getRef(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Blob getBlob(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Clob getClob(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Array getArray(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Date getDate(String parameterName, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Time getTime(String parameterName, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default URL getURL(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default RowId getRowId(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default RowId getRowId(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setRowId(String parameterName, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setNString(String parameterName, String value) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setNCharacterStream(String parameterName, Reader value, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setNClob(String parameterName, NClob value) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setClob(String parameterName, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setBlob(String parameterName, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setNClob(String parameterName, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default NClob getNClob(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default NClob getNClob(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default SQLXML getSQLXML(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default SQLXML getSQLXML(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default String getNString(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default String getNString(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Reader getNCharacterStream(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Reader getNCharacterStream(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Reader getCharacterStream(int parameterIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Reader getCharacterStream(String parameterName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setBlob(String parameterName, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setClob(String parameterName, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setAsciiStream(String parameterName, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setBinaryStream(String parameterName, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setCharacterStream(String parameterName, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setAsciiStream(String parameterName, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setBinaryStream(String parameterName, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setCharacterStream(String parameterName, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setNCharacterStream(String parameterName, Reader value) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setClob(String parameterName, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setBlob(String parameterName, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setNClob(String parameterName, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default <T> T getObject(String parameterName, Class<T> type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int executeUpdate() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setNull(int parameterIndex, int sqlType) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setBoolean(int parameterIndex, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setByte(int parameterIndex, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setShort(int parameterIndex, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setInt(int parameterIndex, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setLong(int parameterIndex, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setFloat(int parameterIndex, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setDouble(int parameterIndex, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setString(int parameterIndex, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setBytes(int parameterIndex, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setDate(int parameterIndex, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setTime(int parameterIndex, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void clearParameters() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setObject(int parameterIndex, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean execute() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void addBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setRef(int parameterIndex, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setBlob(int parameterIndex, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setClob(int parameterIndex, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setArray(int parameterIndex, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default ResultSetMetaData getMetaData() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setURL(int parameterIndex, URL x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default ParameterMetaData getParameterMetaData() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setRowId(int parameterIndex, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setNString(int parameterIndex, String value) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setNClob(int parameterIndex, NClob value) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setNClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default ResultSet executeQuery(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int executeUpdate(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void close() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int getMaxFieldSize() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setMaxFieldSize(int max) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int getMaxRows() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setMaxRows(int max) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setEscapeProcessing(boolean enable) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int getQueryTimeout() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setQueryTimeout(int seconds) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void cancel() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default SQLWarning getWarnings() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void clearWarnings() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setCursorName(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean execute(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default ResultSet getResultSet() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int getUpdateCount() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean getMoreResults() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setFetchDirection(int direction) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int getFetchDirection() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setFetchSize(int rows) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int getFetchSize() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int getResultSetConcurrency() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int getResultSetType() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void addBatch(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void clearBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int[] executeBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean getMoreResults(int current) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int getResultSetHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean isClosed() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setPoolable(boolean poolable) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean isPoolable() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void closeOnCompletion() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean isCloseOnCompletion() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }
}
