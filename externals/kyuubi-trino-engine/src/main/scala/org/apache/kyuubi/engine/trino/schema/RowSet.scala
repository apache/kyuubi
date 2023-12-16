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

package org.apache.kyuubi.engine.trino.schema

import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import io.trino.client.ClientStandardTypes._
import io.trino.client.ClientTypeSignature
import io.trino.client.Row

object RowSet {

  /**
   * A simpler impl of Trino's toHiveString
   */
  def toHiveString(data: Any, typ: ClientTypeSignature): String = {
    (data, typ.getRawType) match {
      case (null, _) =>
        // Only match nulls in nested type values
        "null"

      case (bin: Array[Byte], VARBINARY) =>
        new String(bin, StandardCharsets.UTF_8)

      case (s: String, VARCHAR) =>
        // Only match string in nested type values
        "\"" + s + "\""

      case (list: java.util.List[_], ARRAY) =>
        require(
          typ.getArgumentsAsTypeSignatures.asScala.nonEmpty,
          "Missing ARRAY argument type")
        val listType = typ.getArgumentsAsTypeSignatures.get(0)
        list.asScala
          .map(toHiveString(_, listType))
          .mkString("[", ",", "]")

      case (m: java.util.Map[_, _], MAP) =>
        require(
          typ.getArgumentsAsTypeSignatures.size() == 2,
          "Mismatched number of MAP argument types")
        val keyType = typ.getArgumentsAsTypeSignatures.get(0)
        val valueType = typ.getArgumentsAsTypeSignatures.get(1)
        m.asScala.map { case (key, value) =>
          toHiveString(key, keyType) + ":" + toHiveString(value, valueType)
        }.toSeq.sorted.mkString("{", ",", "}")

      case (row: Row, ROW) =>
        require(
          row.getFields.size() == typ.getArguments.size(),
          "Mismatched data values and ROW type")
        row.getFields.asScala.zipWithIndex.map { case (r, index) =>
          val namedRowType = typ.getArguments.get(index).getNamedTypeSignature
          if (namedRowType.getName.isPresent) {
            namedRowType.getName.get() + "=" +
              toHiveString(r.getValue, namedRowType.getTypeSignature)
          } else {
            toHiveString(r.getValue, namedRowType.getTypeSignature)
          }
        }.mkString("{", ",", "}")

      case (other, _) =>
        other.toString
    }
  }
}
