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

package org.apache.kyuubi.sql.zorder

import java.lang.{Double => jDouble, Float => jFloat}

import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.kyuubi.sql.KyuubiSQLExtensionException

object ZorderBytesUtils {
  private final val BIT_8_MASK = 1 << 7
  private final val BIT_16_MASK = 1 << 15
  private final val BIT_32_MASK = 1 << 31
  private final val BIT_64_MASK = 1L << 63

  def interleaveBits(inputs: Array[Any]): Array[Byte] = {
    inputs.length match {
      // it's a more fast approach, use O(8 * 8)
      // can see http://graphics.stanford.edu/~seander/bithacks.html#InterleaveTableObvious
      case 1 => longToByte(toLong(inputs(0)))
      case 2 => interleave2Longs(toLong(inputs(0)), toLong(inputs(1)))
      case 3 => interleave3Longs(toLong(inputs(0)), toLong(inputs(1)), toLong(inputs(2)))
      case 4 => interleave4Longs(toLong(inputs(0)), toLong(inputs(1)), toLong(inputs(2)),
        toLong(inputs(3)))
      case 5 => interleave5Longs(toLong(inputs(0)), toLong(inputs(1)), toLong(inputs(2)),
        toLong(inputs(3)), toLong(inputs(4)))
      case 6 => interleave6Longs(toLong(inputs(0)), toLong(inputs(1)), toLong(inputs(2)),
        toLong(inputs(3)), toLong(inputs(4)), toLong(inputs(5)))
      case 7 => interleave7Longs(toLong(inputs(0)), toLong(inputs(1)), toLong(inputs(2)),
        toLong(inputs(3)), toLong(inputs(4)), toLong(inputs(5)), toLong(inputs(6)))
      case 8 => interleave8Longs(toLong(inputs(0)), toLong(inputs(1)), toLong(inputs(2)),
        toLong(inputs(3)), toLong(inputs(4)), toLong(inputs(5)), toLong(inputs(6)),
        toLong(inputs(7)))

      case _ =>
        // it's the default approach, use O(64 * n), n is the length of inputs
        interleaveBitsDefault(inputs.map(toByteArray))
    }
  }

  private def interleave2Longs(l1: Long, l2: Long): Array[Byte] = {
    // output 8 * 16 bits
    val result = new Array[Byte](16)
    var i = 0
    while(i < 8) {
      val tmp1 = ((l1 >> (i * 8)) & 0xff).toShort
      val tmp2 = ((l2 >> (i * 8)) & 0xff).toShort

      var z = 0
      var j = 0
      while (j < 8) {
        val x_masked = tmp1 & (1 << j)
        val y_masked = tmp2 & (1 << j)
        z |= (x_masked << j)
        z |= (y_masked << (j + 1))
        j = j + 1
      }
      result((7 - i) * 2 + 1) = (z & 0xff).toByte
      result((7 - i) * 2) = ((z >> 8) & 0xff).toByte
      i = i + 1
    }
    result
  }

  private def interleave3Longs(l1: Long, l2: Long, l3: Long): Array[Byte] = {
    // output 8 * 24 bits
    val result = new Array[Byte](24)
    var i = 0
    while(i < 8) {
      val tmp1 = ((l1 >> (i * 8)) & 0xff).toInt
      val tmp2 = ((l2 >> (i * 8)) & 0xff).toInt
      val tmp3 = ((l3 >> (i * 8)) & 0xff).toInt

      var z = 0
      var j = 0
      while (j < 8) {
        val r1_mask = tmp1 & (1 << j)
        val r2_mask = tmp2 & (1 << j)
        val r3_mask = tmp3 & (1 << j)
        z |= (r1_mask << (2 * j)) | (r2_mask << (2 * j + 1)) | (r3_mask << (2 * j + 2))
        j = j + 1
      }
      result((7 - i) * 3 + 2) = (z & 0xff).toByte
      result((7 - i) * 3 + 1) = ((z >> 8) & 0xff).toByte
      result((7 - i) * 3) = ((z >> 16) & 0xff).toByte
      i = i + 1
    }
    result
  }

  private def interleave4Longs(l1: Long, l2: Long, l3: Long, l4: Long): Array[Byte] = {
    // output 8 * 32 bits
    val result = new Array[Byte](32)
    var i = 0
    while(i < 8) {
      val tmp1 = ((l1 >> (i * 8)) & 0xff).toInt
      val tmp2 = ((l2 >> (i * 8)) & 0xff).toInt
      val tmp3 = ((l3 >> (i * 8)) & 0xff).toInt
      val tmp4 = ((l4 >> (i * 8)) & 0xff).toInt

      var z = 0
      var j = 0
      while (j < 8) {
        val r1_mask = tmp1 & (1 << j)
        val r2_mask = tmp2 & (1 << j)
        val r3_mask = tmp3 & (1 << j)
        val r4_mask = tmp4 & (1 << j)
        z |= (r1_mask << (3 * j)) | (r2_mask << (3 * j + 1)) | (r3_mask << (3 * j + 2)) |
          (r4_mask << (3 * j + 3))
        j = j + 1
      }
      result((7 - i) * 4 + 3) = (z & 0xff).toByte
      result((7 - i) * 4 + 2) = ((z >> 8) & 0xff).toByte
      result((7 - i) * 4 + 1) = ((z >> 16) & 0xff).toByte
      result((7 - i) * 4) = ((z >> 24) & 0xff).toByte
      i = i + 1
    }
    result
  }

  private def interleave5Longs(
      l1: Long,
      l2: Long,
      l3: Long,
      l4: Long,
      l5: Long): Array[Byte] = {
    // output 8 * 40 bits
    val result = new Array[Byte](40)
    var i = 0
    while(i < 8) {
      val tmp1 = ((l1 >> (i * 8)) & 0xff).toLong
      val tmp2 = ((l2 >> (i * 8)) & 0xff).toLong
      val tmp3 = ((l3 >> (i * 8)) & 0xff).toLong
      val tmp4 = ((l4 >> (i * 8)) & 0xff).toLong
      val tmp5 = ((l5 >> (i * 8)) & 0xff).toLong

      var z = 0L
      var j = 0
      while (j < 8) {
        val r1_mask = tmp1 & (1 << j)
        val r2_mask = tmp2 & (1 << j)
        val r3_mask = tmp3 & (1 << j)
        val r4_mask = tmp4 & (1 << j)
        val r5_mask = tmp5 & (1 << j)
        z |= (r1_mask << (4 * j)) | (r2_mask << (4 * j + 1)) | (r3_mask << (4 * j + 2)) |
          (r4_mask << (4 * j + 3)) | (r5_mask << (4 * j + 4))
        j = j + 1
      }
      result((7 - i) * 5 + 4) = (z & 0xff).toByte
      result((7 - i) * 5 + 3) = ((z >> 8) & 0xff).toByte
      result((7 - i) * 5 + 2) = ((z >> 16) & 0xff).toByte
      result((7 - i) * 5 + 1) = ((z >> 24) & 0xff).toByte
      result((7 - i) * 5) = ((z >> 32) & 0xff).toByte
      i = i + 1
    }
    result
  }

  private def interleave6Longs(
      l1: Long,
      l2: Long,
      l3: Long,
      l4: Long,
      l5: Long,
      l6: Long): Array[Byte] = {
    // output 8 * 48 bits
    val result = new Array[Byte](48)
    var i = 0
    while(i < 8) {
      val tmp1 = ((l1 >> (i * 8)) & 0xff).toLong
      val tmp2 = ((l2 >> (i * 8)) & 0xff).toLong
      val tmp3 = ((l3 >> (i * 8)) & 0xff).toLong
      val tmp4 = ((l4 >> (i * 8)) & 0xff).toLong
      val tmp5 = ((l5 >> (i * 8)) & 0xff).toLong
      val tmp6 = ((l6 >> (i * 8)) & 0xff).toLong

      var z = 0L
      var j = 0
      while (j < 8) {
        val r1_mask = tmp1 & (1 << j)
        val r2_mask = tmp2 & (1 << j)
        val r3_mask = tmp3 & (1 << j)
        val r4_mask = tmp4 & (1 << j)
        val r5_mask = tmp5 & (1 << j)
        val r6_mask = tmp6 & (1 << j)
        z |= (r1_mask << (5 * j)) | (r2_mask << (5 * j + 1)) | (r3_mask << (5 * j + 2)) |
          (r4_mask << (5 * j + 3)) | (r5_mask << (5 * j + 4)) | (r6_mask << (5 * j + 5))
        j = j + 1
      }
      result((7 - i) * 6 + 5) = (z & 0xff).toByte
      result((7 - i) * 6 + 4) = ((z >> 8) & 0xff).toByte
      result((7 - i) * 6 + 3) = ((z >> 16) & 0xff).toByte
      result((7 - i) * 6 + 2) = ((z >> 24) & 0xff).toByte
      result((7 - i) * 6 + 1) = ((z >> 32) & 0xff).toByte
      result((7 - i) * 6) = ((z >> 40) & 0xff).toByte
      i = i + 1
    }
    result
  }

  private def interleave7Longs(
      l1: Long,
      l2: Long,
      l3: Long,
      l4: Long,
      l5: Long,
      l6: Long,
      l7: Long): Array[Byte] = {
    // output 8 * 56 bits
    val result = new Array[Byte](56)
    var i = 0
    while(i < 8) {
      val tmp1 = ((l1 >> (i * 8)) & 0xff).toLong
      val tmp2 = ((l2 >> (i * 8)) & 0xff).toLong
      val tmp3 = ((l3 >> (i * 8)) & 0xff).toLong
      val tmp4 = ((l4 >> (i * 8)) & 0xff).toLong
      val tmp5 = ((l5 >> (i * 8)) & 0xff).toLong
      val tmp6 = ((l6 >> (i * 8)) & 0xff).toLong
      val tmp7 = ((l7 >> (i * 8)) & 0xff).toLong

      var z = 0L
      var j = 0
      while (j < 8) {
        val r1_mask = tmp1 & (1 << j)
        val r2_mask = tmp2 & (1 << j)
        val r3_mask = tmp3 & (1 << j)
        val r4_mask = tmp4 & (1 << j)
        val r5_mask = tmp5 & (1 << j)
        val r6_mask = tmp6 & (1 << j)
        val r7_mask = tmp7 & (1 << j)
        z |= (r1_mask << (6 * j)) | (r2_mask << (6 * j + 1)) | (r3_mask << (6 * j + 2)) |
          (r4_mask << (6 * j + 3)) | (r5_mask << (6 * j + 4)) | (r6_mask << (6 * j + 5)) |
          (r7_mask << (6 * j + 6))
        j = j + 1
      }
      result((7 - i) * 7 + 6) = (z & 0xff).toByte
      result((7 - i) * 7 + 5) = ((z >> 8) & 0xff).toByte
      result((7 - i) * 7 + 4) = ((z >> 16) & 0xff).toByte
      result((7 - i) * 7 + 3) = ((z >> 24) & 0xff).toByte
      result((7 - i) * 7 + 2) = ((z >> 32) & 0xff).toByte
      result((7 - i) * 7 + 1) = ((z >> 40) & 0xff).toByte
      result((7 - i) * 7) = ((z >> 48) & 0xff).toByte
      i = i + 1
    }
    result
  }

  private def interleave8Longs(
      l1: Long,
      l2: Long,
      l3: Long,
      l4: Long,
      l5: Long,
      l6: Long,
      l7: Long,
      l8: Long): Array[Byte] = {
    // output 8 * 64 bits
    val result = new Array[Byte](64)
    var i = 0
    while(i < 8) {
      val tmp1 = ((l1 >> (i * 8)) & 0xff).toLong
      val tmp2 = ((l2 >> (i * 8)) & 0xff).toLong
      val tmp3 = ((l3 >> (i * 8)) & 0xff).toLong
      val tmp4 = ((l4 >> (i * 8)) & 0xff).toLong
      val tmp5 = ((l5 >> (i * 8)) & 0xff).toLong
      val tmp6 = ((l6 >> (i * 8)) & 0xff).toLong
      val tmp7 = ((l7 >> (i * 8)) & 0xff).toLong
      val tmp8 = ((l8 >> (i * 8)) & 0xff).toLong

      var z = 0L
      var j = 0
      while (j < 8) {
        val r1_mask = tmp1 & (1 << j)
        val r2_mask = tmp2 & (1 << j)
        val r3_mask = tmp3 & (1 << j)
        val r4_mask = tmp4 & (1 << j)
        val r5_mask = tmp5 & (1 << j)
        val r6_mask = tmp6 & (1 << j)
        val r7_mask = tmp7 & (1 << j)
        val r8_mask = tmp8 & (1 << j)
        z |= (r1_mask << (7 * j)) | (r2_mask << (7 * j + 1)) | (r3_mask << (7 * j + 2)) |
          (r4_mask << (7 * j + 3)) | (r5_mask << (7 * j + 4)) | (r6_mask << (7 * j + 5)) |
          (r7_mask << (7 * j + 6)) | (r8_mask << (7 * j + 7))
        j = j + 1
      }
      result((7 - i) * 8 + 7) = (z & 0xff).toByte
      result((7 - i) * 8 + 6) = ((z >> 8) & 0xff).toByte
      result((7 - i) * 8 + 5) = ((z >> 16) & 0xff).toByte
      result((7 - i) * 8 + 4) = ((z >> 24) & 0xff).toByte
      result((7 - i) * 8 + 3) = ((z >> 32) & 0xff).toByte
      result((7 - i) * 8 + 2) = ((z >> 40) & 0xff).toByte
      result((7 - i) * 8 + 1) = ((z >> 48) & 0xff).toByte
      result((7 - i) * 8) = ((z >> 56) & 0xff).toByte
      i = i + 1
    }
    result
  }

  def interleaveBitsDefault(arrays: Array[Array[Byte]]): Array[Byte] = {
    var totalLength = 0
    var maxLength = 0
    arrays.foreach { array =>
      totalLength += array.length
      maxLength = maxLength.max(array.length * 8)
    }
    val result = new Array[Byte](totalLength)
    var resultBit = 0

    var bit = 0
    while (bit < maxLength) {
      val bytePos = bit / 8
      val bitPos = bit % 8

      for (arr <- arrays) {
        val len = arr.length
        if (bytePos < len) {
          val resultBytePos = totalLength - 1 - resultBit / 8
          val resultBitPos = resultBit % 8
          result(resultBytePos) = updatePos(result(resultBytePos), resultBitPos,
            arr(len - 1 - bytePos), bitPos)
          resultBit += 1
        }
      }
      bit += 1
    }
    result
  }

  def updatePos(a: Byte, apos: Int, b: Byte, bpos: Int): Byte = {
    var temp = (b & (1 << bpos)).toByte
    if (apos > bpos) {
      temp = (temp << (apos - bpos)).toByte
    } else if (apos < bpos) {
      temp = (temp >> (bpos - apos)).toByte
    }
    val atemp = (a & (1 << apos)).toByte
    if (atemp == temp) {
      return a
    }
    (a ^ (1 << apos)).toByte
  }

  def toLong(a: Any): Long = {
    a match {
      case b: Boolean => (if (b) 1 else 0).toLong ^ BIT_64_MASK
      case b: Byte => b.toLong ^ BIT_64_MASK
      case s: Short => s.toLong ^ BIT_64_MASK
      case i: Int => i.toLong ^ BIT_64_MASK
      case l: Long => l ^ BIT_64_MASK
      case f: Float => f.toLong ^ BIT_64_MASK
      case d: Double => d.toLong ^ BIT_64_MASK
      case str: UTF8String => str.getPrefix
      case dec: Decimal => dec.toLong ^ BIT_64_MASK
      case other: Any =>
        throw new KyuubiSQLExtensionException("Unsupported z-order type: " + other.getClass)
    }
  }

  def toByteArray(a: Any): Array[Byte] = {
    a match {
      case bo: Boolean =>
        booleanToByte(bo)
      case b: Byte =>
        byteToByte(b)
      case s: Short =>
        shortToByte(s)
      case i: Int =>
        intToByte(i)
      case l: Long =>
        longToByte(l)
      case f: Float =>
        floatToByte(f)
      case d: Double =>
        doubleToByte(d)
      case str: UTF8String =>
        // truncate or padding str to 8 byte
        paddingTo8Byte(str.getBytes)
      case dec: Decimal =>
        longToByte(dec.toLong)
      case other: Any =>
        throw new KyuubiSQLExtensionException("Unsupported z-order type: " + other.getClass)
    }
  }

  def booleanToByte(a: Boolean): Array[Byte] = {
    if (a) {
      byteToByte(1.toByte)
    } else {
      byteToByte(0.toByte)
    }
  }

  def byteToByte(a: Byte): Array[Byte] = {
    val tmp = (a ^ BIT_8_MASK).toByte
    Array(tmp)
  }

  def shortToByte(a: Short): Array[Byte] = {
    val tmp = a ^ BIT_16_MASK
    Array(((tmp >> 8) & 0xff).toByte, (tmp & 0xff).toByte)
  }

  def intToByte(a: Int): Array[Byte] = {
    val result = new Array[Byte](4)
    var i = 0
    val tmp = a ^ BIT_32_MASK
    while (i <= 3) {
      val offset = i * 8
      result(3 - i) = ((tmp >> offset) & 0xff).toByte
      i += 1
    }
    result
  }

  def longToByte(a: Long): Array[Byte] = {
    val result = new Array[Byte](8)
    var i = 0
    val tmp = a ^ BIT_64_MASK
    while (i <= 7) {
      val offset = i * 8
      result(7 - i) = ((tmp >> offset) & 0xff).toByte
      i += 1
    }
    result
  }

  def floatToByte(a: Float): Array[Byte] = {
    val fi = jFloat.floatToRawIntBits(a)
    intToByte(fi)
  }

  def doubleToByte(a: Double): Array[Byte] = {
    val dl = jDouble.doubleToRawLongBits(a)
    longToByte(dl)
  }

  def paddingTo8Byte(a: Array[Byte]): Array[Byte] = {
    val len = a.length
    if (len == 8) {
      a
    } else if (len > 8) {
      val result = new Array[Byte](8)
      System.arraycopy(a, 0, result, 0, 8)
      result
    } else {
      val result = new Array[Byte](8)
      System.arraycopy(a, 0, result, 8 - len, len)
      result
    }
  }

  def defaultByteArrayValue(dataType: DataType): Array[Byte] = toByteArray {
    defaultValue(dataType)
  }

  def defaultValue(dataType: DataType): Any = {
    dataType match {
      case BooleanType =>
        true
      case ByteType =>
        Byte.MaxValue
      case ShortType =>
        Short.MaxValue
      case IntegerType | DateType =>
        Int.MaxValue
      case LongType | TimestampType | _: DecimalType =>
        Long.MaxValue
      case FloatType =>
        Float.MaxValue
      case DoubleType =>
        Double.MaxValue
      case StringType =>
        // we pad string to 8 bytes so it's equal to long
        UTF8String.fromBytes(longToByte(Long.MaxValue))
      case other: Any =>
        throw new KyuubiSQLExtensionException(s"Unsupported z-order type: ${other.catalogString}")
    }
  }
}
