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

import java.util.Collections
import java.util.Locale

import scala.collection.JavaConverters._

import io.trino.client.ClientStandardTypes._
import io.trino.client.ClientTypeSignature
import io.trino.client.Column

import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TCLIServiceConstants
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TColumnDesc
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TPrimitiveTypeEntry
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTableSchema
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeDesc
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeEntry
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeQualifiers
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeQualifierValue

object SchemaHelper {

  private[schema] val UNKNOWN: String = "unknown"

  private lazy val STRING_TYPES = Set(
    HYPER_LOG_LOG,
    QDIGEST,
    P4_HYPER_LOG_LOG,
    TIMESTAMP_WITH_TIME_ZONE,
    TIME,
    TIME_WITH_TIME_ZONE,
    JSON,
    IPADDRESS,
    UUID,
    GEOMETRY,
    SPHERICAL_GEOGRAPHY,
    BING_TILE)

  def toTTypeId(typ: ClientTypeSignature): TTypeId = typ.getRawType.toLowerCase(Locale.ROOT) match {
    case BOOLEAN => TTypeId.BOOLEAN_TYPE
    case TINYINT => TTypeId.TINYINT_TYPE
    case SMALLINT => TTypeId.SMALLINT_TYPE
    case INTEGER => TTypeId.INT_TYPE
    case BIGINT => TTypeId.BIGINT_TYPE
    case REAL => TTypeId.FLOAT_TYPE
    case DOUBLE => TTypeId.DOUBLE_TYPE
    case DECIMAL => TTypeId.DECIMAL_TYPE
    case CHAR => TTypeId.CHAR_TYPE
    case VARCHAR => TTypeId.VARCHAR_TYPE
    case VARBINARY => TTypeId.BINARY_TYPE
    case DATE => TTypeId.DATE_TYPE
    case TIMESTAMP => TTypeId.TIMESTAMP_TYPE
    case INTERVAL_DAY_TO_SECOND => TTypeId.INTERVAL_DAY_TIME_TYPE
    case INTERVAL_YEAR_TO_MONTH => TTypeId.INTERVAL_YEAR_MONTH_TYPE
    case ARRAY => TTypeId.ARRAY_TYPE
    case MAP => TTypeId.MAP_TYPE
    case ROW => TTypeId.STRUCT_TYPE
    case stringType if STRING_TYPES.contains(stringType) => TTypeId.STRING_TYPE
    case UNKNOWN => TTypeId.NULL_TYPE
    case other =>
      throw new IllegalArgumentException(s"Unrecognized trino type name: $other")
  }

  def toTTypeQualifiers(typ: ClientTypeSignature): TTypeQualifiers = {
    val ret = new TTypeQualifiers()
    val qualifiers = typ.getRawType match {
      case DECIMAL =>
        Map(
          TCLIServiceConstants.PRECISION ->
            TTypeQualifierValue.i32Value(typ.getArguments.get(0).getValue.asInstanceOf[Long].toInt),
          TCLIServiceConstants.SCALE ->
            TTypeQualifierValue.i32Value(typ.getArguments.get(1).getValue.asInstanceOf[Long].toInt))
          .asJava
      case _ => Collections.emptyMap[String, TTypeQualifierValue]()
    }
    ret.setQualifiers(qualifiers)
    ret
  }

  def toTTypeDesc(typ: ClientTypeSignature): TTypeDesc = {
    val typeEntry = new TPrimitiveTypeEntry(toTTypeId(typ))
    typeEntry.setTypeQualifiers(toTTypeQualifiers(typ))
    val tTypeDesc = new TTypeDesc()
    tTypeDesc.addToTypes(TTypeEntry.primitiveEntry(typeEntry))
    tTypeDesc
  }

  def toTColumnDesc(column: Column, pos: Int): TColumnDesc = {
    val tColumnDesc = new TColumnDesc()
    tColumnDesc.setColumnName(column.getName)
    tColumnDesc.setTypeDesc(toTTypeDesc(column.getTypeSignature))
    tColumnDesc.setPosition(pos)
    tColumnDesc
  }

  def toTTableSchema(schema: Seq[Column]): TTableSchema = {
    val tTableSchema = new TTableSchema()
    schema.zipWithIndex.foreach { case (f, i) =>
      tTableSchema.addToColumns(toTColumnDesc(f, i))
    }
    tTableSchema
  }
}
