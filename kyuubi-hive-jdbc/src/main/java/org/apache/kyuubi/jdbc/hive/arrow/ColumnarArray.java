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

public final class ColumnarArray {
  // The data for this array. This array contains elements from
  // data[offset] to data[offset + length).
  private final ArrowColumnVector data;
  private final int offset;
  private final int length;

  public ColumnarArray(ArrowColumnVector data, int offset, int length) {
    this.data = data;
    this.offset = offset;
    this.length = length;
  }

  public int numElements() {
    return length;
  }

  public boolean[] toBooleanArray() {
    return data.getBooleans(offset, length);
  }

  public byte[] toByteArray() {
    return data.getBytes(offset, length);
  }

  public short[] toShortArray() {
    return data.getShorts(offset, length);
  }

  public int[] toIntArray() {
    return data.getInts(offset, length);
  }

  public long[] toLongArray() {
    return data.getLongs(offset, length);
  }

  public float[] toFloatArray() {
    return data.getFloats(offset, length);
  }

  public double[] toDoubleArray() {
    return data.getDoubles(offset, length);
  }

  // TODO: this is extremely expensive.
  //  public Object[] array() {
  //    DataType dt = data.dataType();
  //    Object[] list = new Object[length];
  //    try {
  //      for (int i = 0; i < length; i++) {
  //        if (!data.isNullAt(offset + i)) {
  //          list[i] = get(i, dt);
  //        }
  //      }
  //      return list;
  //    } catch(Exception e) {
  //      throw new RuntimeException("Could not get the array", e);
  //    }
  //  }
}
