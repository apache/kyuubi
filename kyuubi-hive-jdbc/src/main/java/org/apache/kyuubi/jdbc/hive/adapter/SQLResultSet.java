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
import java.util.Map;

@SuppressWarnings("deprecation")
public interface SQLResultSet extends ResultSet {

  @Override
  default byte[] getBytes(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Time getTime(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default InputStream getAsciiStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default InputStream getUnicodeStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default byte[] getBytes(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Time getTime(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default InputStream getAsciiStream(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default InputStream getUnicodeStream(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default String getCursorName() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Reader getCharacterStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Reader getCharacterStream(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean isBeforeFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean isAfterLast() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean isFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean isLast() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void beforeFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void afterLast() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean first() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean last() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int getRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean absolute(int row) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean relative(int rows) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean previous() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void setFetchDirection(int direction) throws SQLException {
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
  default void updateNull(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateByte(int columnIndex, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateShort(int columnIndex, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateInt(int columnIndex, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateLong(int columnIndex, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateFloat(int columnIndex, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateDouble(int columnIndex, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateString(int columnIndex, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateDate(int columnIndex, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateTime(int columnIndex, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateObject(int columnIndex, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateNull(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateByte(String columnLabel, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateShort(String columnLabel, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateInt(String columnLabel, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateLong(String columnLabel, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateFloat(String columnLabel, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateDouble(String columnLabel, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateString(String columnLabel, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateDate(String columnLabel, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateTime(String columnLabel, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateAsciiStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateObject(String columnLabel, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void insertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void deleteRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void refreshRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void cancelRowUpdates() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void moveToInsertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void moveToCurrentRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Ref getRef(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Blob getBlob(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Clob getClob(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Array getArray(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Ref getRef(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Blob getBlob(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Clob getClob(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Array getArray(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default URL getURL(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default URL getURL(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateRef(int columnIndex, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateRef(String columnLabel, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateBlob(String columnLabel, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateClob(int columnIndex, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateClob(String columnLabel, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateArray(int columnIndex, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateArray(String columnLabel, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default RowId getRowId(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default RowId getRowId(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default int getHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default boolean isClosed() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateNString(int columnIndex, String nString) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateNString(String columnLabel, String nString) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default NClob getNClob(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default NClob getNClob(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default SQLXML getSQLXML(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default SQLXML getSQLXML(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default String getNString(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default String getNString(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Reader getNCharacterStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default Reader getNCharacterStream(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  default <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
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
