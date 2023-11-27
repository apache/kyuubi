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

import scala.collection.JavaConverters._

import io.trino.client.ClientStandardTypes._
import io.trino.client.Column

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.engine.trino.schema.SchemaHelper._
import org.apache.kyuubi.engine.trino.util.TestUtils._
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TCLIServiceConstants
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId

class SchemaHelperSuite extends KyuubiFunSuite {

  val outerSchema: Seq[Column] =
    List(
      column("c0", BOOLEAN),
      column("c1", TINYINT),
      column("c2", SMALLINT),
      column("c3", INTEGER),
      column("c4", BIGINT),
      column("c5", REAL),
      column("c6", DOUBLE),
      column("c7", DECIMAL, decimalTypeSignature),
      column("c8", CHAR),
      column("c9", VARCHAR),
      column("c10", VARBINARY),
      column("c11", DATE),
      column("c12", TIMESTAMP),
      column("c13", INTERVAL_DAY_TO_SECOND),
      column("c14", INTERVAL_YEAR_TO_MONTH),
      column("c15", ARRAY, arrayTypeSignature),
      column("c16", MAP, mapTypeSignature),
      column("c17", ROW, rowTypeSignature),
      column("c18", TIME_WITH_TIME_ZONE),
      column("c19", TIMESTAMP_WITH_TIME_ZONE),
      column("c20", IPADDRESS),
      column("c21", UUID),
      column("c22", UNKNOWN))

  test("toTTypeId") {
    assert(toTTypeId(outerSchema.head.getTypeSignature) === TTypeId.BOOLEAN_TYPE)
    assert(toTTypeId(outerSchema(1).getTypeSignature) === TTypeId.TINYINT_TYPE)
    assert(toTTypeId(outerSchema(2).getTypeSignature) === TTypeId.SMALLINT_TYPE)
    assert(toTTypeId(outerSchema(3).getTypeSignature) === TTypeId.INT_TYPE)
    assert(toTTypeId(outerSchema(4).getTypeSignature) === TTypeId.BIGINT_TYPE)
    assert(toTTypeId(outerSchema(5).getTypeSignature) === TTypeId.FLOAT_TYPE)
    assert(toTTypeId(outerSchema(6).getTypeSignature) === TTypeId.DOUBLE_TYPE)
    assert(toTTypeId(outerSchema(7).getTypeSignature) === TTypeId.DECIMAL_TYPE)
    assert(toTTypeId(outerSchema(8).getTypeSignature) === TTypeId.CHAR_TYPE)
    assert(toTTypeId(outerSchema(9).getTypeSignature) === TTypeId.VARCHAR_TYPE)
    assert(toTTypeId(outerSchema(10).getTypeSignature) === TTypeId.BINARY_TYPE)
    assert(toTTypeId(outerSchema(11).getTypeSignature) === TTypeId.DATE_TYPE)
    assert(toTTypeId(outerSchema(12).getTypeSignature) === TTypeId.TIMESTAMP_TYPE)
    assert(toTTypeId(
      outerSchema(13).getTypeSignature) === TTypeId.INTERVAL_DAY_TIME_TYPE)
    assert(toTTypeId(
      outerSchema(14).getTypeSignature) === TTypeId.INTERVAL_YEAR_MONTH_TYPE)
    assert(toTTypeId(outerSchema(15).getTypeSignature) === TTypeId.ARRAY_TYPE)
    assert(toTTypeId(outerSchema(16).getTypeSignature) === TTypeId.MAP_TYPE)
    assert(toTTypeId(outerSchema(17).getTypeSignature) === TTypeId.STRUCT_TYPE)
    assert(toTTypeId(outerSchema(18).getTypeSignature) === TTypeId.STRING_TYPE)
    assert(toTTypeId(outerSchema(19).getTypeSignature) === TTypeId.STRING_TYPE)
    assert(toTTypeId(outerSchema(20).getTypeSignature) === TTypeId.STRING_TYPE)
    assert(toTTypeId(outerSchema(21).getTypeSignature) === TTypeId.STRING_TYPE)
    assert(toTTypeId(outerSchema(22).getTypeSignature) === TTypeId.NULL_TYPE)

    val e1 = intercept[IllegalArgumentException](toTTypeId(textTypeSignature))
    assert(e1.getMessage === "Unrecognized trino type name: text")
  }

  test("toTTypeQualifiers") {
    val qualifiers = toTTypeQualifiers(outerSchema(7).getTypeSignature)
    val q = qualifiers.getQualifiers
    assert(q.size() === 2)
    assert(q.get(TCLIServiceConstants.PRECISION).getI32Value === 10)
    assert(q.get(TCLIServiceConstants.SCALE).getI32Value === 8)

    outerSchema.map(_.getTypeSignature).foreach {
      case f if f.getRawType == DECIMAL =>
      case f => assert(toTTypeQualifiers(f).getQualifiers.isEmpty)
    }
  }

  test("toTTableSchema") {
    val tTableSchema = toTTableSchema(outerSchema)
    assert(tTableSchema.getColumnsSize === outerSchema.size)
    val iter = tTableSchema.getColumns

    iter.asScala.zipWithIndex.foreach { case (col, pos) =>
      val field = outerSchema(pos)
      assert(col.getColumnName === field.getName)
      assert(col.getPosition === pos)
      val qualifiers =
        col.getTypeDesc.getTypes.get(0).getPrimitiveEntry.getTypeQualifiers.getQualifiers
      if (pos == 7) {
        assert(qualifiers.get(TCLIServiceConstants.PRECISION).getI32Value === 10)
        assert(qualifiers.get(TCLIServiceConstants.SCALE).getI32Value === 8)
      } else {
        assert(qualifiers.isEmpty)
      }
    }
  }
}
