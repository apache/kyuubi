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

import com.google.common.primitives.*;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TColumn;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId;

/** ColumnBuffer */
public class ColumnBuffer extends AbstractList<Object> {

  private final TTypeId type;

  private BitSet nulls;

  private int size;
  private boolean[] boolVars;
  private byte[] byteVars;
  private short[] shortVars;
  private int[] intVars;
  private long[] longVars;
  private double[] doubleVars;
  private List<String> stringVars;
  private List<ByteBuffer> binaryVars;

  public ColumnBuffer(TColumn colValues) {
    if (colValues.isSetBoolVal()) {
      type = TTypeId.BOOLEAN_TYPE;
      nulls = toBitset(colValues.getBoolVal().getNulls());
      boolVars = Booleans.toArray(colValues.getBoolVal().getValues());
      size = boolVars.length;
    } else if (colValues.isSetByteVal()) {
      type = TTypeId.TINYINT_TYPE;
      nulls = toBitset(colValues.getByteVal().getNulls());
      byteVars = Bytes.toArray(colValues.getByteVal().getValues());
      size = byteVars.length;
    } else if (colValues.isSetI16Val()) {
      type = TTypeId.SMALLINT_TYPE;
      nulls = toBitset(colValues.getI16Val().getNulls());
      shortVars = Shorts.toArray(colValues.getI16Val().getValues());
      size = shortVars.length;
    } else if (colValues.isSetI32Val()) {
      type = TTypeId.INT_TYPE;
      nulls = toBitset(colValues.getI32Val().getNulls());
      intVars = Ints.toArray(colValues.getI32Val().getValues());
      size = intVars.length;
    } else if (colValues.isSetI64Val()) {
      type = TTypeId.BIGINT_TYPE;
      nulls = toBitset(colValues.getI64Val().getNulls());
      longVars = Longs.toArray(colValues.getI64Val().getValues());
      size = longVars.length;
    } else if (colValues.isSetDoubleVal()) {
      type = TTypeId.DOUBLE_TYPE;
      nulls = toBitset(colValues.getDoubleVal().getNulls());
      doubleVars = Doubles.toArray(colValues.getDoubleVal().getValues());
      size = doubleVars.length;
    } else if (colValues.isSetBinaryVal()) {
      type = TTypeId.BINARY_TYPE;
      nulls = toBitset(colValues.getBinaryVal().getNulls());
      binaryVars = colValues.getBinaryVal().getValues();
      size = binaryVars.size();
    } else if (colValues.isSetStringVal()) {
      type = TTypeId.STRING_TYPE;
      nulls = toBitset(colValues.getStringVal().getNulls());
      stringVars = colValues.getStringVal().getValues();
      size = stringVars.size();
    } else {
      throw new IllegalStateException("invalid union object");
    }
  }

  @SuppressWarnings("unchecked")
  public ColumnBuffer(TTypeId type, BitSet nulls, Object values) {
    this.type = type;
    this.nulls = nulls;
    if (type == TTypeId.BOOLEAN_TYPE) {
      boolVars = (boolean[]) values;
      size = boolVars.length;
    } else if (type == TTypeId.TINYINT_TYPE) {
      byteVars = (byte[]) values;
      size = byteVars.length;
    } else if (type == TTypeId.SMALLINT_TYPE) {
      shortVars = (short[]) values;
      size = shortVars.length;
    } else if (type == TTypeId.INT_TYPE) {
      intVars = (int[]) values;
      size = intVars.length;
    } else if (type == TTypeId.BIGINT_TYPE) {
      longVars = (long[]) values;
      size = longVars.length;
    } else if (type == TTypeId.DOUBLE_TYPE || type == TTypeId.FLOAT_TYPE) {
      doubleVars = (double[]) values;
      size = doubleVars.length;
    } else if (type == TTypeId.BINARY_TYPE) {
      binaryVars = (List<ByteBuffer>) values;
      size = binaryVars.size();
    } else if (type == TTypeId.STRING_TYPE) {
      stringVars = (List<String>) values;
      size = stringVars.size();
    } else {
      throw new IllegalStateException("invalid union object");
    }
  }

  private static final byte[] MASKS =
      new byte[] {0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, (byte) 0x80};

  private static BitSet toBitset(byte[] nulls) {
    BitSet bitset = new BitSet();
    int bits = nulls.length * 8;
    for (int i = 0; i < bits; i++) {
      bitset.set(i, (nulls[i / 8] & MASKS[i % 8]) != 0);
    }
    return bitset;
  }

  /**
   * Get a subset of this ColumnBuffer, starting from the 1st value.
   *
   * @param end index after the last value to include
   */
  public ColumnBuffer extractSubset(int end) {
    BitSet subNulls = nulls.get(0, end);
    if (type == TTypeId.BOOLEAN_TYPE) {
      ColumnBuffer subset = new ColumnBuffer(type, subNulls, Arrays.copyOfRange(boolVars, 0, end));
      boolVars = Arrays.copyOfRange(boolVars, end, size);
      nulls = nulls.get(end, size);
      size = boolVars.length;
      return subset;
    }
    if (type == TTypeId.TINYINT_TYPE) {
      ColumnBuffer subset = new ColumnBuffer(type, subNulls, Arrays.copyOfRange(byteVars, 0, end));
      byteVars = Arrays.copyOfRange(byteVars, end, size);
      nulls = nulls.get(end, size);
      size = byteVars.length;
      return subset;
    }
    if (type == TTypeId.SMALLINT_TYPE) {
      ColumnBuffer subset = new ColumnBuffer(type, subNulls, Arrays.copyOfRange(shortVars, 0, end));
      shortVars = Arrays.copyOfRange(shortVars, end, size);
      nulls = nulls.get(end, size);
      size = shortVars.length;
      return subset;
    }
    if (type == TTypeId.INT_TYPE) {
      ColumnBuffer subset = new ColumnBuffer(type, subNulls, Arrays.copyOfRange(intVars, 0, end));
      intVars = Arrays.copyOfRange(intVars, end, size);
      nulls = nulls.get(end, size);
      size = intVars.length;
      return subset;
    }
    if (type == TTypeId.BIGINT_TYPE) {
      ColumnBuffer subset = new ColumnBuffer(type, subNulls, Arrays.copyOfRange(longVars, 0, end));
      longVars = Arrays.copyOfRange(longVars, end, size);
      nulls = nulls.get(end, size);
      size = longVars.length;
      return subset;
    }
    if (type == TTypeId.DOUBLE_TYPE || type == TTypeId.FLOAT_TYPE) {
      ColumnBuffer subset =
          new ColumnBuffer(type, subNulls, Arrays.copyOfRange(doubleVars, 0, end));
      doubleVars = Arrays.copyOfRange(doubleVars, end, size);
      nulls = nulls.get(end, size);
      size = doubleVars.length;
      return subset;
    }
    if (type == TTypeId.BINARY_TYPE) {
      ColumnBuffer subset = new ColumnBuffer(type, subNulls, binaryVars.subList(0, end));
      binaryVars = binaryVars.subList(end, binaryVars.size());
      nulls = nulls.get(end, size);
      size = binaryVars.size();
      return subset;
    }
    if (type == TTypeId.STRING_TYPE) {
      ColumnBuffer subset = new ColumnBuffer(type, subNulls, stringVars.subList(0, end));
      stringVars = stringVars.subList(end, stringVars.size());
      nulls = nulls.get(end, size);
      size = stringVars.size();
      return subset;
    }
    throw new IllegalStateException("invalid union object");
  }

  public TTypeId getType() {
    return type;
  }

  @Override
  public Object get(int index) {
    if (nulls.get(index)) {
      return null;
    }
    switch (type) {
      case BOOLEAN_TYPE:
        return boolVars[index];
      case TINYINT_TYPE:
        return byteVars[index];
      case SMALLINT_TYPE:
        return shortVars[index];
      case INT_TYPE:
        return intVars[index];
      case BIGINT_TYPE:
        return longVars[index];
      case FLOAT_TYPE:
      case DOUBLE_TYPE:
        return doubleVars[index];
      case STRING_TYPE:
        return stringVars.get(index);
      case BINARY_TYPE:
        return binaryVars.get(index).array();
      default:
        return null;
    }
  }

  @Override
  public int size() {
    return size;
  }
}
