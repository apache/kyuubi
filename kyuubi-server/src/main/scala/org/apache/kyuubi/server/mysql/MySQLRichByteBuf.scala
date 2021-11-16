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

package org.apache.kyuubi.server.mysql

import java.nio.charset.StandardCharsets

import io.netty.buffer.ByteBuf

// https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol
object MySQLRichByteBuf {

  private def charset = StandardCharsets.UTF_8

  implicit class Implicit(self: ByteBuf) {

    /**
     * Read 1 byte fixed length integer from byte buffers.
     *
     * @return 1 byte fixed length integer
     */
    def readInt1: Int = self.readUnsignedByte

    /**
     * Write 1 byte fixed length integer to byte buffers.
     *
     * @param value 1 byte fixed length integer
     */
    def writeInt1(value: Int): ByteBuf = self.writeByte(value)

    /**
     * Read 2 byte fixed length integer from byte buffers.
     *
     * @return 2 byte fixed length integer
     */
    def readInt2: Int = self.readUnsignedShortLE

    /**
     * Write 2 byte fixed length integer to byte buffers.
     *
     * @param value 2 byte fixed length integer
     */
    def writeInt2(value: Int): ByteBuf = self.writeShortLE(value)

    /**
     * Read 3 byte fixed length integer from byte buffers.
     *
     * @return 3 byte fixed length integer
     */
    def readInt3: Int = self.readUnsignedMediumLE

    /**
     * Write 3 byte fixed length integer to byte buffers.
     *
     * @param value 3 byte fixed length integer
     */
    def writeInt3(value: Int): ByteBuf = self.writeMediumLE(value)

    /**
     * Read 4 byte fixed length integer from byte buffers.
     *
     * @return 4 byte fixed length integer
     */
    def readInt4: Int = self.readIntLE

    /**
     * Write 4 byte fixed length integer to byte buffers.
     *
     * @param value 4 byte fixed length integer
     */
    def writeInt4(value: Int): ByteBuf = self.writeIntLE(value)

    /**
     * Read 6 byte fixed length integer from byte buffers.
     *
     * @return 6 byte fixed length integer
     */
    def readInt6: Long = {
      var result = 0
      var i = 0
      while (i < 6) {
        result |= (0xff & self.readByte).toLong << (8 * i)
        i = i + 1
      }
      result
    }

    /**
     * Write 6 byte fixed length integer to byte buffers.
     *
     * @param value 6 byte fixed length integer
     */
    def writeInt6(value: Long): ByteBuf = throw new UnsupportedOperationException

    /**
     * Read 8 byte fixed length integer from byte buffers.
     *
     * @return 8 byte fixed length integer
     */
    def readInt8: Long = self.readLongLE

    /**
     * Write 8 byte fixed length integer to byte buffers.
     *
     * @param value 8 byte fixed length integer
     */
    def writeInt8(value: Long): ByteBuf = self.writeLongLE(value)

    /**
     * Read lenenc integer from byte buffers.
     *
     * @return lenenc integer
     */
    def readIntLenenc: Long = {
      val firstByte = readInt1
      if (firstByte < 0xfb) return firstByte
      if (0xfb == firstByte) return 0
      if (0xfc == firstByte) return readInt2
      if (0xfd == firstByte) return readInt3
      self.readLongLE
    }

    /**
     * Write lenenc integer to byte buffers.
     *
     * @param value lenenc integer
     */
    def writeIntLenenc(value: Long): ByteBuf = {
      if (value < 0xfb) {
        self.writeByte(value.toInt)
      } else if (value < (1 << 16)) {
        self.writeByte(0xfc)
        self.writeShortLE(value.toInt)
      } else if (value < (1 << 24)) {
        self.writeByte(0xfd)
        self.writeMediumLE(value.toInt)
      } else {
        self.writeByte(0xfe)
        self.writeLongLE(value)
      }
    }

    /**
     * Read fixed length long from byte buffers.
     *
     * @param length length read from byte buffers
     * @return fixed length long
     */
    def readLong(length: Int): Long = {
      var result = 0
      var i = 0
      while (i < length) {
        result = result << 8 | readInt1
        i = i + 1
      }
      result
    }

    /**
     * Read lenenc string from byte buffers.
     *
     * @return lenenc string
     */
    def readStringLenenc: String = {
      val length = readIntLenenc.toInt
      val result = new Array[Byte](length)
      self.readBytes(result)
      new String(result, charset)
    }

    /**
     * Read lenenc string from byte buffers for bytes.
     *
     * @return lenenc bytes
     */
    def readStringLenencByBytes: Array[Byte] = {
      val length = readIntLenenc.toInt
      val result = new Array[Byte](length)
      self.readBytes(result)
      result
    }

    /**
     * Write lenenc string to byte buffers.
     *
     * @param value fixed length string
     */
    def writeStringLenenc(value: String): ByteBuf = {
      val bytes = value.getBytes(charset)
      writeIntLenenc(bytes.length)
      self.writeBytes(bytes)
    }

    /**
     * Write lenenc bytes to byte buffers.
     *
     * @param value fixed length bytes
     */
    def writeBytesLenenc(value: Array[Byte]): ByteBuf = {
      if (0 == value.length) {
        self.writeByte(0)
        return self
      }
      writeIntLenenc(value.length)
      self.writeBytes(value)
    }

    /**
     * Read fixed length string from byte buffers.
     *
     * @param length length of fixed string
     * @return fixed length string
     */
    def readStringFix(length: Int): String = new String(readStringFixByBytes(length), charset)

    /**
     * Read fixed length string from byte buffers and return bytes.
     *
     * @param length length of fixed string
     * @return fixed length bytes
     */
    def readStringFixByBytes(length: Int): Array[Byte] = {
      val result = new Array[Byte](length)
      self.readBytes(result)
      result
    }

    /**
     * Write variable length string to byte buffers.
     *
     * @param value fixed length string
     */
    def writeStringFix(value: String): ByteBuf = self.writeBytes(value.getBytes(charset))

    /**
     * Write variable length bytes to byte buffers.
     *
     * @param value fixed length bytes
     */
    def writeBytes(value: Array[Byte]): ByteBuf = self.writeBytes(value)

    /**
     * Read null terminated string from byte buffers.
     *
     * @return null terminated string
     */
    def readStringNul: String = new String(readStringNulByBytes, charset)

    /**
     * Read null terminated string from byte buffers and return bytes.
     *
     * @return null terminated bytes
     */
    def readStringNulByBytes: Array[Byte] = {
      val result = new Array[Byte](self.bytesBefore(0.toByte))
      self.readBytes(result)
      self.skipBytes(1)
      result
    }

    /**
     * Write null terminated string to byte buffers.
     *
     * @param value null terminated string
     */
    def writeStringNul(value: String): ByteBuf = {
      self.writeBytes(value.getBytes(charset))
      self.writeByte(0)
    }

    /**
     * Read rest of packet string from byte buffers and return bytes.
     *
     * @return rest of packet string bytes
     */
    def readStringEOFByBytes: Array[Byte] = {
      val result = new Array[Byte](self.readableBytes)
      self.readBytes(result)
      result
    }

    /**
     * Read rest of packet string from byte buffers.
     *
     * @return rest of packet string
     */
    def readStringEOF: String = {
      val result = new Array[Byte](self.readableBytes)
      self.readBytes(result)
      new String(result, charset)
    }

    /**
     * Write rest of packet string to byte buffers.
     *
     * @param value rest of packet string
     */
    def writeStringEOF(value: String): ByteBuf = self.writeBytes(value.getBytes(charset))

    /**
     * Skip reserved from byte buffers.
     *
     * @param length length of reserved
     */
    def skipReserved(length: Int): ByteBuf = self.skipBytes(length)

    /**
     * Write null for reserved to byte buffers.
     *
     * @param length length of reserved
     */
    def writeReserved(length: Int): ByteBuf = self.writeZero(length)
  }
}
