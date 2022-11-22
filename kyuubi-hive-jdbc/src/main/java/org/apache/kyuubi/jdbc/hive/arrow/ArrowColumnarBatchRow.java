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

package org.apache.kyuubi.jdbc.hive.arrow;

import java.math.BigDecimal;
import java.sql.Timestamp;
import org.apache.hive.service.rpc.thrift.TTypeId;
import org.apache.kyuubi.jdbc.hive.common.DateUtils;
import org.apache.kyuubi.jdbc.hive.common.HiveIntervalDayTime;
import org.apache.kyuubi.jdbc.hive.common.HiveIntervalYearMonth;

public class ArrowColumnarBatchRow {
  public int rowId;
  private final ArrowColumnVector[] columns;

  public ArrowColumnarBatchRow(ArrowColumnVector[] columns) {
    this.columns = columns;
  }

  public int numFields() {
    return columns.length;
  }

  public boolean anyNull() {
    throw new UnsupportedOperationException();
  }

  public boolean isNullAt(int ordinal) {
    return columns[ordinal].isNullAt(rowId);
  }

  public boolean getBoolean(int ordinal) {
    return columns[ordinal].getBoolean(rowId);
  }

  public byte getByte(int ordinal) {
    return columns[ordinal].getByte(rowId);
  }

  public short getShort(int ordinal) {
    return columns[ordinal].getShort(rowId);
  }

  public int getInt(int ordinal) {
    return columns[ordinal].getInt(rowId);
  }

  public long getLong(int ordinal) {
    return columns[ordinal].getLong(rowId);
  }

  public float getFloat(int ordinal) {
    return columns[ordinal].getFloat(rowId);
  }

  public double getDouble(int ordinal) {
    return columns[ordinal].getDouble(rowId);
  }

  public BigDecimal getDecimal(int ordinal, int precision, int scale) {
    return columns[ordinal].getDecimal(rowId, precision, scale);
  }

  public BigDecimal getDecimal(int ordinal) {
    return columns[ordinal].getDecimal(rowId);
  }

  public String getString(int ordinal) {
    return columns[ordinal].getString(rowId);
  }

  public byte[] getBinary(int ordinal) {
    return columns[ordinal].getBinary(rowId);
  }

  public Object getInterval(int ordinal) {
    throw new UnsupportedOperationException();
  }

  public Object getStruct(int ordinal, int numFields) {
    throw new UnsupportedOperationException();
  }

  public Object getArray(int ordinal) {
    throw new UnsupportedOperationException();
  }

  public Object getMap(int ordinal) {
    throw new UnsupportedOperationException();
  }

  public Object get(int ordinal, TTypeId dataType) {
    switch (dataType) {
      case BOOLEAN_TYPE:
        return getBoolean(ordinal);
      case TINYINT_TYPE:
        return getByte(ordinal);
      case SMALLINT_TYPE:
        return getShort(ordinal);
      case INT_TYPE:
        return getInt(ordinal);
      case BIGINT_TYPE:
        return getLong(ordinal);
      case BINARY_TYPE:
        return getBinary(ordinal);
      case FLOAT_TYPE:
        return getFloat(ordinal);
      case DOUBLE_TYPE:
        return getDouble(ordinal);
      case DECIMAL_TYPE:
        return getDecimal(ordinal);
      case STRING_TYPE:
        return getString(ordinal);
      case TIMESTAMP_TYPE:
        return new Timestamp(getLong(ordinal) / 1000);
      case DATE_TYPE:
        return DateUtils.internalToDate(getInt(ordinal));
      case INTERVAL_DAY_TIME_TYPE:
        long microseconds = getLong(ordinal);
        long seconds = microseconds / 1000000;
        int nanos = (int) (microseconds % 1000000) * 1000;
        return new HiveIntervalDayTime(seconds, nanos);
      case INTERVAL_YEAR_MONTH_TYPE:
        return new HiveIntervalYearMonth(getInt(ordinal));
      case ARRAY_TYPE:
        return getString(ordinal);
      case MAP_TYPE:
        return getString(ordinal);
      case STRUCT_TYPE:
        return getString(ordinal);
      default:
        throw new UnsupportedOperationException("Datatype not supported " + dataType);
    }
  }
}
