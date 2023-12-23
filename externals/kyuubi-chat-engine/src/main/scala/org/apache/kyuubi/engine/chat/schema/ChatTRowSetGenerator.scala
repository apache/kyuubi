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

package org.apache.kyuubi.engine.chat.schema

import org.apache.kyuubi.engine.chat.schema.ChatTRowSetGenerator._
import org.apache.kyuubi.engine.result.TRowSetGenerator
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

class ChatTRowSetGenerator
  extends TRowSetGenerator[Seq[String], Seq[String], String] {

  override def getColumnSizeFromSchemaType(schema: Seq[String]): Int = schema.length

  override def getColumnType(schema: Seq[String], ordinal: Int): String = COL_STRING_TYPE

  override def isColumnNullAt(row: Seq[String], ordinal: Int): Boolean = row(ordinal) == null

  override def getColumnAs[T](row: Seq[String], ordinal: Int): T = row(ordinal).asInstanceOf[T]

  override def toTColumn(rows: Seq[Seq[String]], ordinal: Int, typ: String): TColumn =
    typ match {
      case COL_STRING_TYPE => asStringTColumn(rows, ordinal)
      case otherType => throw new UnsupportedOperationException(s"type $otherType")
    }

  override def toTColumnValue(row: Seq[String], ordinal: Int, types: Seq[String]): TColumnValue =
    getColumnType(types, ordinal) match {
      case COL_STRING_TYPE => asStringTColumnValue(row, ordinal)
      case otherType => throw new UnsupportedOperationException(s"type $otherType")
    }
}

object ChatTRowSetGenerator {
  val COL_STRING_TYPE: String = classOf[String].getSimpleName
}
