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

package org.apache.kyuubi.jdbc.hive.cli;

import org.apache.kyuubi.shaded.hive.service.rpc.thrift.*;

/** Protocols before HIVE_CLI_SERVICE_PROTOCOL_V6 (used by RowBasedSet) */
public class ColumnValue {

  private static Boolean getBooleanValue(TBoolValue tBoolValue) {
    if (tBoolValue.isSetValue()) {
      return tBoolValue.isValue();
    }
    return null;
  }

  private static Byte getByteValue(TByteValue tByteValue) {
    if (tByteValue.isSetValue()) {
      return tByteValue.getValue();
    }
    return null;
  }

  private static Short getShortValue(TI16Value tI16Value) {
    if (tI16Value.isSetValue()) {
      return tI16Value.getValue();
    }
    return null;
  }

  private static Integer getIntegerValue(TI32Value tI32Value) {
    if (tI32Value.isSetValue()) {
      return tI32Value.getValue();
    }
    return null;
  }

  private static Long getLongValue(TI64Value tI64Value) {
    if (tI64Value.isSetValue()) {
      return tI64Value.getValue();
    }
    return null;
  }

  private static Double getDoubleValue(TDoubleValue tDoubleValue) {
    if (tDoubleValue.isSetValue()) {
      return tDoubleValue.getValue();
    }
    return null;
  }

  private static String getStringValue(TStringValue tStringValue) {
    if (tStringValue.isSetValue()) {
      return tStringValue.getValue();
    }
    return null;
  }

  public static Object toColumnValue(TColumnValue value) {
    TColumnValue._Fields field = value.getSetField();
    switch (field) {
      case BOOL_VAL:
        return getBooleanValue(value.getBoolVal());
      case BYTE_VAL:
        return getByteValue(value.getByteVal());
      case I16_VAL:
        return getShortValue(value.getI16Val());
      case I32_VAL:
        return getIntegerValue(value.getI32Val());
      case I64_VAL:
        return getLongValue(value.getI64Val());
      case DOUBLE_VAL:
        return getDoubleValue(value.getDoubleVal());
      case STRING_VAL:
        return getStringValue(value.getStringVal());
    }
    throw new IllegalArgumentException("never");
  }
}
