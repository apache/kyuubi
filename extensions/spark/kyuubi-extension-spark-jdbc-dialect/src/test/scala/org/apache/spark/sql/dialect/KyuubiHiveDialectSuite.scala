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

package org.apache.spark.sql.dialect

import org.apache.spark.sql.dialect.KyuubiHiveDialect._
import org.apache.spark.sql.types._
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

class KyuubiHiveDialectSuite extends AnyFunSuite {
// scalastyle:on

  test("[KYUUBI #3489] Kyuubi Hive dialect: can handle jdbc url") {
    assert(canHandle("jdbc:hive2://"))
    assert(canHandle("jdbc:kyuubi://"))
  }

  test("[KYUUBI #3489] Kyuubi Hive dialect: quoteIdentifier") {
    assertResult("`id`")(quoteIdentifier("id"))
    assertResult("`table`.`id`")(quoteIdentifier("table.id"))
  }

  test("KYUUBI #3942 adapt to Hive data type definitions") {
    def getJdbcTypeDefinition(dt: DataType): String = {
      getJDBCType(dt).get.databaseTypeDefinition
    }
    assertResult("INT")(getJdbcTypeDefinition(IntegerType))
    assertResult("DOUBLE")(getJdbcTypeDefinition(DoubleType))
    assertResult("FLOAT")(getJdbcTypeDefinition(FloatType))
    assertResult("TINYINT")(getJdbcTypeDefinition(ByteType))
    assertResult("BOOLEAN")(getJdbcTypeDefinition(BooleanType))
    assertResult("STRING")(getJdbcTypeDefinition(StringType))
    assertResult("BINARY")(getJdbcTypeDefinition(BinaryType))
    assertResult("DATE")(getJdbcTypeDefinition(DateType))
  }
}
