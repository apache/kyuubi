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

import io.netty.buffer.ByteBuf

import org.apache.kyuubi.server.mysql.MySQLRichByteBuf.Implicit

object MySQLNullBitmap {

  def apply(columnsNumbers: Int, offset: Int): MySQLNullBitmap = {
    val nullBitmap = new Array[Int](calculateBytes(columnsNumbers, offset))
    MySQLNullBitmap(offset, nullBitmap)
  }

  def apply(columnNumbers: Int, payload: ByteBuf): MySQLNullBitmap = {
    val offset = 0
    val nullBitmap = new Array[Int](calculateBytes(columnNumbers, 0))
    fillBitmap(nullBitmap, payload)
    MySQLNullBitmap(offset, nullBitmap)
  }

  private def calculateBytes(columnsNumbers: Int, offset: Int): Int =
    (columnsNumbers + offset + 7) / 8

  private def fillBitmap(nullBitmap: Array[Int], payload: ByteBuf): Unit = {
    nullBitmap.indices.foreach(i => nullBitmap(i) = payload.readInt1)
  }
}

case class MySQLNullBitmap private(
  offset: Int,
  nullBitmap: Array[Int] // it's not too efficient but convenient
) {
  def isNullParameter(index: Int): Boolean =
    (nullBitmap(getBytePos(index)) & (1 << getBitPos(index))) != 0

  def setNullBit(index: Int): Unit =
    nullBitmap(getBytePos(index)) |= 1 << getBitPos(index)

  private def getBytePos(index: Int): Int = (index + offset) / 8

  private def getBitPos(index: Int): Int = (index + offset) % 8
}
