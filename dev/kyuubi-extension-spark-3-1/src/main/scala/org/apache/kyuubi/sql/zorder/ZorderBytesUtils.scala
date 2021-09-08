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
import java.nio.charset.Charset

import org.apache.spark.sql.types.Decimal

import org.apache.kyuubi.sql.KyuubiSQLExtensionException

object ZorderBytesUtils {
  def interleaveMultiByteArray(arrays: Array[Array[Byte]]): Array[Byte] = {
    var totalLength = 0
    var maxLength = 0
    arrays.foreach(array => {
      totalLength += array.length
      maxLength = maxLength.max(array.length * 8)
    })
    val result = new Array[Byte](totalLength)
    var resultBit = 0

    var bit = 0
    while (bit < maxLength) {
      val bytePos = Math.floor(bit / 8).toInt
      val bitPos = bit % 8

      for (arr <- arrays) {
        if (bytePos < arr.length) {
          val resultBytePos = totalLength - 1 - Math.floor(resultBit / 8).toInt
          val resultBitPos = resultBit % 8
          result(resultBytePos) = updatePos(result(resultBytePos), resultBitPos,
            arr(arr.length - 1 - bytePos), bitPos)
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

  def toByte(a: Any): Array[Byte] = {
    a match {
      case bo: Boolean =>
        ZorderBytesUtils.booleanToByte(bo)
      case b: Byte =>
        ZorderBytesUtils.byteToByte(b)
      case s: Short =>
        ZorderBytesUtils.shortToByte(s)
      case i: Int =>
        ZorderBytesUtils.intToByte(i)
      case l: Long =>
        ZorderBytesUtils.longToByte(l)
      case f: Float =>
        ZorderBytesUtils.floatToByte(f)
      case d: Double =>
        ZorderBytesUtils.doubleToByte(d)
      case str: String =>
        ZorderBytesUtils.stringToByte(str)
      case dec: Decimal =>
        ZorderBytesUtils.longToByte(dec.toLong)
      case other: Any =>
        throw new KyuubiSQLExtensionException("Unsupported z-order type: " + other.getClass)
    }
  }

  def booleanToByte(a: Boolean): Array[Byte] = {
    if (a) {
      intToByte(1)
    } else {
      intToByte(0)
    }
  }

  def byteToByte(a: Int): Array[Byte] = {
    val tmp = (a ^ (1 << 7)).toByte
    Array(tmp)
  }

  def shortToByte(a: Short): Array[Byte] = {
    val tmp = a ^ (1 << 15)
    Array(((tmp >> 8) & 0xff).toByte, (tmp & 0xff).toByte)
  }

  def intToByte(a: Int): Array[Byte] = {
    val tmp = a ^ (1 << 31)
    val result = new Array[Byte](4)
    var i = 0
    while (i < 3) {
      val offset = i * 8
      result(3 - i) = ((tmp >> offset) & 0xff).toByte
      i += 1
    }
    result
  }

  def longToByte(a: Long): Array[Byte] = {
    val tmp = a ^ (1 << 63)
    val result = new Array[Byte](8)
    var i = 0
    while (i < 7) {
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

  def stringToByte(str: String): Array[Byte] = {
    // truncate or padding str to 8 byte
    paddingTo8Byte(str.getBytes(Charset.forName("utf-8")))
  }

  def paddingTo8Byte(a: Array[Byte]): Array[Byte] = {
    if (a.length == 8) {
      return a
    }
    if (a.length > 8) {
      val result = new Array[Byte](8);
      a.copyToArray(result)
      return result
    }
    val paddingSize = 8 - a.length;
    val emptyArray = Array.ofDim[Byte](paddingSize)
    arrayConcat(emptyArray, a)
  }

  def arrayConcat(bytes: Array[Byte]*): Array[Byte] = {
    val length = bytes.foldLeft(0)(_ + _.length)
    val result = new Array[Byte](length)
    var pos = 0
    bytes.foreach(arr => {
      arr.copyToArray(result, pos)
      pos += arr.length
    })
    result
  }
}
