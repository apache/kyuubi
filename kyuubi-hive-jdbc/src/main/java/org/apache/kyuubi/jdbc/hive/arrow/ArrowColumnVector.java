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
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

/** A column vector backed by Apache Arrow. */
public class ArrowColumnVector {

  ArrowVectorAccessor accessor;
  ArrowColumnVector[] childColumns;

  public ValueVector getValueVector() {
    return accessor.vector;
  }

  public boolean hasNull() {
    return accessor.getNullCount() > 0;
  }

  public int numNulls() {
    return accessor.getNullCount();
  }

  public void close() {
    if (childColumns != null) {
      for (int i = 0; i < childColumns.length; i++) {
        childColumns[i].close();
        childColumns[i] = null;
      }
      childColumns = null;
    }
    accessor.close();
  }

  public boolean isNullAt(int rowId) {
    return accessor.isNullAt(rowId);
  }

  public boolean getBoolean(int rowId) {
    return accessor.getBoolean(rowId);
  }

  public byte getByte(int rowId) {
    return accessor.getByte(rowId);
  }

  public short getShort(int rowId) {
    return accessor.getShort(rowId);
  }

  public int getInt(int rowId) {
    return accessor.getInt(rowId);
  }

  public long getLong(int rowId) {
    return accessor.getLong(rowId);
  }

  public float getFloat(int rowId) {
    return accessor.getFloat(rowId);
  }

  public double getDouble(int rowId) {
    return accessor.getDouble(rowId);
  }

  public BigDecimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) return null;
    return accessor.getDecimal(rowId, precision, scale);
  }

  public BigDecimal getDecimal(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getDecimal(rowId);
  }

  public String getString(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getString(rowId);
  }

  public byte[] getBinary(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getBinary(rowId);
  }

  public ArrowColumnVector(ValueVector vector) {
    initAccessor(vector);
  }

  void initAccessor(ValueVector vector) {
    if (vector instanceof BitVector) {
      accessor = new BooleanAccessor((BitVector) vector);
    } else if (vector instanceof TinyIntVector) {
      accessor = new ByteAccessor((TinyIntVector) vector);
    } else if (vector instanceof SmallIntVector) {
      accessor = new ShortAccessor((SmallIntVector) vector);
    } else if (vector instanceof IntVector) {
      accessor = new IntAccessor((IntVector) vector);
    } else if (vector instanceof BigIntVector) {
      accessor = new LongAccessor((BigIntVector) vector);
    } else if (vector instanceof Float4Vector) {
      accessor = new FloatAccessor((Float4Vector) vector);
    } else if (vector instanceof Float8Vector) {
      accessor = new DoubleAccessor((Float8Vector) vector);
    } else if (vector instanceof DecimalVector) {
      accessor = new DecimalAccessor((DecimalVector) vector);
    } else if (vector instanceof VarCharVector) {
      accessor = new StringAccessor((VarCharVector) vector);
    } else if (vector instanceof VarBinaryVector) {
      accessor = new BinaryAccessor((VarBinaryVector) vector);
    } else if (vector instanceof DateDayVector) {
      accessor = new DateAccessor((DateDayVector) vector);
    } else if (vector instanceof TimeStampMicroTZVector) {
      accessor = new TimestampAccessor((TimeStampMicroTZVector) vector);
    } else if (vector instanceof TimeStampMicroVector) {
      accessor = new TimestampNTZAccessor((TimeStampMicroVector) vector);
    } else if (vector instanceof NullVector) {
      accessor = new NullAccessor((NullVector) vector);
    } else if (vector instanceof IntervalYearVector) {
      accessor = new IntervalYearAccessor((IntervalYearVector) vector);
    } else if (vector instanceof DurationVector) {
      accessor = new DurationAccessor((DurationVector) vector);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  abstract static class ArrowVectorAccessor {

    final ValueVector vector;

    ArrowVectorAccessor(ValueVector vector) {
      this.vector = vector;
    }

    final boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    final int getNullCount() {
      return vector.getNullCount();
    }

    final void close() {
      vector.close();
    }

    boolean getBoolean(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte getByte(int rowId) {
      throw new UnsupportedOperationException();
    }

    short getShort(int rowId) {
      throw new UnsupportedOperationException();
    }

    int getInt(int rowId) {
      throw new UnsupportedOperationException();
    }

    long getLong(int rowId) {
      throw new UnsupportedOperationException();
    }

    float getFloat(int rowId) {
      throw new UnsupportedOperationException();
    }

    double getDouble(int rowId) {
      throw new UnsupportedOperationException();
    }

    BigDecimal getDecimal(int rowId, int precision, int scale) {
      throw new UnsupportedOperationException();
    }

    BigDecimal getDecimal(int rowId) {
      throw new UnsupportedOperationException();
    }

    String getString(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte[] getBinary(int rowId) {
      throw new UnsupportedOperationException();
    }

    Object getArray(int rowId) {
      throw new UnsupportedOperationException();
    }

    Object getMap(int rowId) {
      throw new UnsupportedOperationException();
    }
  }

  static class BooleanAccessor extends ArrowVectorAccessor {

    private final BitVector accessor;

    BooleanAccessor(BitVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final boolean getBoolean(int rowId) {
      return accessor.get(rowId) == 1;
    }
  }

  static class ByteAccessor extends ArrowVectorAccessor {

    private final TinyIntVector accessor;

    ByteAccessor(TinyIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte getByte(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class ShortAccessor extends ArrowVectorAccessor {

    private final SmallIntVector accessor;

    ShortAccessor(SmallIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final short getShort(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class IntAccessor extends ArrowVectorAccessor {

    private final IntVector accessor;

    IntAccessor(IntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class LongAccessor extends ArrowVectorAccessor {

    private final BigIntVector accessor;

    LongAccessor(BigIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class FloatAccessor extends ArrowVectorAccessor {

    private final Float4Vector accessor;

    FloatAccessor(Float4Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final float getFloat(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class DoubleAccessor extends ArrowVectorAccessor {

    private final Float8Vector accessor;

    DoubleAccessor(Float8Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final double getDouble(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class DecimalAccessor extends ArrowVectorAccessor {

    private final DecimalVector accessor;

    DecimalAccessor(DecimalVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final BigDecimal getDecimal(int rowId, int precision, int scale) {
      if (isNullAt(rowId)) return null;
      return accessor.getObject(rowId);
    }

    @Override
    final BigDecimal getDecimal(int rowId) {
      if (isNullAt(rowId)) return null;
      return accessor.getObject(rowId);
    }
  }

  static class StringAccessor extends ArrowVectorAccessor {

    private final VarCharVector accessor;
    private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

    StringAccessor(VarCharVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final String getString(int rowId) {
      accessor.get(rowId, stringResult);
      if (stringResult.isSet == 0) {
        return null;
      } else {
        return new String(accessor.get(rowId));
      }
    }
  }

  static class BinaryAccessor extends ArrowVectorAccessor {

    private final VarBinaryVector accessor;

    BinaryAccessor(VarBinaryVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte[] getBinary(int rowId) {
      return accessor.getObject(rowId);
    }
  }

  static class DateAccessor extends ArrowVectorAccessor {

    private final DateDayVector accessor;

    DateAccessor(DateDayVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class TimestampAccessor extends ArrowVectorAccessor {

    private final TimeStampMicroTZVector accessor;

    TimestampAccessor(TimeStampMicroTZVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class TimestampNTZAccessor extends ArrowVectorAccessor {

    private final TimeStampMicroVector accessor;

    TimestampNTZAccessor(TimeStampMicroVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class NullAccessor extends ArrowVectorAccessor {

    NullAccessor(NullVector vector) {
      super(vector);
    }
  }

  static class IntervalYearAccessor extends ArrowVectorAccessor {

    private final IntervalYearVector accessor;

    IntervalYearAccessor(IntervalYearVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class DurationAccessor extends ArrowVectorAccessor {

    private final DurationVector accessor;

    DurationAccessor(DurationVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return DurationVector.get(accessor.getDataBuffer(), rowId);
    }
  }
}
