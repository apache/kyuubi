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
package org.apache.kyuubi.engine.jdbc.impala

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException}
import org.apache.kyuubi.engine.jdbc.dialect.ImpalaDialect
import org.apache.kyuubi.engine.jdbc.impala.DialectSuite.{SCHEME_NAME, TABLE_NAME}

class DialectSuite extends KyuubiFunSuite {

  private val dialect: ImpalaDialect = new ImpalaDialect()

  test("impala dialect - get tables query") {
    val expectedQuery = "show tables"
    val actualQuery = dialect.getTablesQuery(null, null, null, null)

    assert(expectedQuery == actualQuery.trim)
  }

  test("impala dialect - get tables query by scheme") {
    val expectedQuery = s"show tables in $SCHEME_NAME"
    val actualQuery = dialect.getTablesQuery(null, SCHEME_NAME, null, null)

    assert(expectedQuery == actualQuery.trim)
  }

  test("impala dialect - get tables query by table name") {
    val expectedQuery = s"show tables like '$TABLE_NAME'"
    val queryWithTableName = dialect.getTablesQuery(null, null, TABLE_NAME, null)

    assert(expectedQuery == queryWithTableName.trim)

    // kyuubi injects '%' in case if schema is null
    val queryWithWildcardScheme = dialect.getTablesQuery(null, "%", TABLE_NAME, null)

    assert(expectedQuery == queryWithWildcardScheme.trim)
  }

  test("impala dialect - get tables query by scheme and table name") {
    val expectedQuery = s"show tables in $SCHEME_NAME like 'test*'"
    val actualQuery = dialect.getTablesQuery(null, SCHEME_NAME, "test*", null)

    assert(expectedQuery == actualQuery.trim)
  }

  test("impala dialect - fail get tables if pattern-like scheme provided") {
    val exception = intercept[KyuubiSQLException] {
      dialect.getTablesQuery(null, "*scheme*", TABLE_NAME, null)
    }
    assert(exception.getMessage == "Pattern-like schema names not supported")
  }

  test("impala dialect - get columns query by table name") {
    val expectedQuery = s"show column stats $TABLE_NAME"
    val actualQuery = dialect.getColumnsQuery(null, null, null, TABLE_NAME, null)

    assert(expectedQuery == actualQuery.trim)
  }

  test("impala dialect - get columns query by scheme and table name") {
    val expectedQuery = s"show column stats $SCHEME_NAME.$TABLE_NAME"
    val actualQuery = dialect.getColumnsQuery(null, null, SCHEME_NAME, TABLE_NAME, null)

    assert(expectedQuery == actualQuery.trim)
  }

  test("impala dialect - fail get columns if pattern-like scheme provided") {
    val exception = intercept[KyuubiSQLException] {
      dialect.getColumnsQuery(null, null, "*scheme*", TABLE_NAME, null)
    }
    assert(exception.getMessage == "Pattern-like schema names not supported")
  }

  test("impala dialect - fail get columns if pattern-like table provided") {
    val exception = intercept[KyuubiSQLException] {
      dialect.getColumnsQuery(null, null, null, "*test*", null)
    }
    assert(exception.getMessage == "Pattern-like table names not supported")
  }

  test("impala dialect - fail get columns if table name not provided") {
    val exception = intercept[KyuubiSQLException] {
      dialect.getColumnsQuery(null, null, null, null, null)
    }
    assert(exception.getMessage == "Table name should not be empty")
  }
}

object DialectSuite {
  val TABLE_NAME = "test_table"
  val SCHEME_NAME = "test_db"
}
