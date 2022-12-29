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

package org.apache.kyuubi.parser.trino

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.sql.parser.trino.KyuubiTrinoFeParser
import org.apache.kyuubi.sql.plan.trino.GetSchemas

class KyuubiTrinoFeParserSuite extends KyuubiFunSuite {
  val parser = new KyuubiTrinoFeParser()

  test("get schemas") {
    def check(query: String, catalog: String = null, schema: String = null): Unit = {
      parser.parsePlan(query) match {
        case GetSchemas(catalogName, schemaPattern) =>
          assert(catalogName == catalog)
          assert(schemaPattern == schema)
        case _ => throw new IllegalStateException()
      }
    }

    check(
      """
        |SELECT TABLE_SCHEM, TABLE_CATALOG FROM system.jdbc.schemas
        |ORDER BY TABLE_CATALOG, TABLE_SCHEM
        |""".stripMargin)

    check(
      """
        |SELECT TABLE_SCHEM, TABLE_CATALOG FROM system.jdbc.schemas
        |WHERE TABLE_CATALOG='aaa'
        |ORDER BY TABLE_CATALOG, TABLE_SCHEM
        |""".stripMargin,
      catalog = "aaa")

    check(
      """
        |SELECT TABLE_SCHEM, TABLE_CATALOG FROM system.jdbc.schemas
        |WHERE TABLE_SCHEM LIKE 'aa%'
        |ORDER BY TABLE_CATALOG, TABLE_SCHEM
        |""".stripMargin,
      schema = "aa%")

    check(
      """
        |SELECT TABLE_SCHEM, TABLE_CATALOG FROM system.jdbc.schemas
        |WHERE TABLE_CATALOG='bb' and TABLE_SCHEM LIKE 'bb%'
        |ORDER BY TABLE_CATALOG, TABLE_SCHEM
        |""".stripMargin,
      catalog = "bb",
      schema = "bb%")
  }
}
