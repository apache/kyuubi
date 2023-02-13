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
import org.apache.kyuubi.sql.plan.{KyuubiTreeNode, PassThroughNode}
import org.apache.kyuubi.sql.plan.trino.{GetCatalogs, GetColumns, GetPrimaryKeys, GetSchemas, GetTables, GetTableTypes, GetTypeInfo}

class KyuubiTrinoFeParserSuite extends KyuubiFunSuite {
  val parser = new KyuubiTrinoFeParser()

  private def parse(sql: String): KyuubiTreeNode = {
    parser.parsePlan(sql)
  }

  test("get schemas") {
    def check(query: String, catalog: String = null, schema: String = null): Unit = {
      parse(query) match {
        case GetSchemas(catalogName, schemaPattern) =>
          assert(catalogName == catalog)
          assert(schemaPattern == schema)
        case _ => fail(s"Query $query parse failed. ")
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
        |WHERE TABLE_CATALOG IS NULL
        |ORDER BY TABLE_CATALOG, TABLE_SCHEM
        |""".stripMargin)

    check(
      """
        |SELECT TABLE_SCHEM, TABLE_CATALOG FROM system.jdbc.schemas
        |WHERE TABLE_SCHEM LIKE 'aa%' ESCAPE '\'
        |ORDER BY TABLE_CATALOG, TABLE_SCHEM
        |""".stripMargin,
      schema = "aa%")

    check(
      """
        |SELECT TABLE_SCHEM, TABLE_CATALOG FROM system.jdbc.schemas
        |WHERE TABLE_CATALOG='bb' and TABLE_SCHEM LIKE 'bb%' ESCAPE '\'
        |ORDER BY TABLE_CATALOG, TABLE_SCHEM
        |""".stripMargin,
      catalog = "bb",
      schema = "bb%")
  }

  test("Parse PassThroughNode") {
    assert(parse("yikaifei").isInstanceOf[PassThroughNode])

    assert(parse("SELECT * FROM T1").isInstanceOf[PassThroughNode])
  }

  test("Support GetCatalogs for Trino Fe") {
    val kyuubiTreeNode = parse(
      """
        |SELECT TABLE_CAT FROM system.jdbc.catalogs ORDER BY TABLE_CAT
        |""".stripMargin)

    assert(kyuubiTreeNode.isInstanceOf[GetCatalogs])
  }

  test("Support GetTableTypes for Trino Fe") {
    val kyuubiTreeNode = parse(
      """
        |SELECT TABLE_TYPE FROM system.jdbc.table_types ORDER BY TABLE_TYPE
        |""".stripMargin)

    assert(kyuubiTreeNode.isInstanceOf[GetTableTypes])
  }

  test("Support GetTypeInfo for Trino Fe") {
    val kyuubiTreeNode = parse(
      """
        |SELECT TYPE_NAME, DATA_TYPE, PRECISION, LITERAL_PREFIX, LITERAL_SUFFIX,
        |CREATE_PARAMS, NULLABLE, CASE_SENSITIVE, SEARCHABLE, UNSIGNED_ATTRIBUTE,
        |FIXED_PREC_SCALE, AUTO_INCREMENT, LOCAL_TYPE_NAME, MINIMUM_SCALE, MAXIMUM_SCALE,
        |SQL_DATA_TYPE, SQL_DATETIME_SUB, NUM_PREC_RADIX
        |FROM system.jdbc.types
        |ORDER BY DATA_TYPE
        |""".stripMargin)

    assert(kyuubiTreeNode.isInstanceOf[GetTypeInfo])
  }

  test("Support GetTables for Trino Fe") {
    def check(
        query: String,
        catalog: String = null,
        schema: String = null,
        tableName: String = null,
        types: List[String] = null,
        emptyRes: Boolean = false): Unit = {
      parse(query) match {
        case GetTables(catalogName, schemaPattern, tableNamePattern, tableTypes, emptyResult) =>
          assert(catalog == catalogName)
          assert(schema == schemaPattern)
          assert(tableName == tableNamePattern)
          assert(types == tableTypes)
          assert(emptyRes == emptyResult)
        case _ => fail(s"Query $query parse failed. ")
      }
    }

    check(
      """
        | SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS,
        | TYPE_CAT, TYPE_SCHEM, TYPE_NAME,
        | SELF_REFERENCING_COL_NAME, REF_GENERATION
        | FROM system.jdbc.tables
        | ORDER BY TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME
        |""".stripMargin)

    check(
      """
        | SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS,
        | TYPE_CAT, TYPE_SCHEM, TYPE_NAME,
        | SELF_REFERENCING_COL_NAME, REF_GENERATION
        | FROM system.jdbc.tables
        | WHERE TABLE_CAT IS NULL
        | ORDER BY TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME
        |""".stripMargin)

    check(
      """
        | SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS,
        | TYPE_CAT, TYPE_SCHEM, TYPE_NAME,
        | SELF_REFERENCING_COL_NAME, REF_GENERATION
        | FROM system.jdbc.tables
        | WHERE TABLE_CAT = 'ykf_catalog'
        | ORDER BY TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME
        |""".stripMargin,
      catalog = "ykf_catalog")

    check(
      """
        | SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS,
        | TYPE_CAT, TYPE_SCHEM, TYPE_NAME,
        | SELF_REFERENCING_COL_NAME, REF_GENERATION
        | FROM system.jdbc.tables
        | WHERE TABLE_CAT = 'ykf_catalog' AND TABLE_SCHEM IS NULL
        | ORDER BY TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME
        |""".stripMargin,
      catalog = "ykf_catalog")

    check(
      """
        | SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS,
        | TYPE_CAT, TYPE_SCHEM, TYPE_NAME,
        | SELF_REFERENCING_COL_NAME, REF_GENERATION
        | FROM system.jdbc.tables
        | WHERE TABLE_CAT = 'ykf_catalog' AND TABLE_SCHEM LIKE '%aa' ESCAPE '\'
        | ORDER BY TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME
        |""".stripMargin,
      catalog = "ykf_catalog",
      "%aa")

    check(
      """
        | SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS,
        | TYPE_CAT, TYPE_SCHEM, TYPE_NAME,
        | SELF_REFERENCING_COL_NAME, REF_GENERATION
        | FROM system.jdbc.tables
        | WHERE TABLE_CAT = 'ykf_catalog' AND TABLE_NAME LIKE '%aa' ESCAPE '\'
        | ORDER BY TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME
        |""".stripMargin,
      catalog = "ykf_catalog",
      tableName = "%aa")

    check(
      """
        | SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS,
        | TYPE_CAT, TYPE_SCHEM, TYPE_NAME,
        | SELF_REFERENCING_COL_NAME, REF_GENERATION
        | FROM system.jdbc.tables
        | WHERE TABLE_CAT = 'ykf_catalog' AND TABLE_NAME LIKE '%aa' ESCAPE '\' AND FALSE
        | ORDER BY TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME
        |""".stripMargin,
      catalog = "ykf_catalog",
      tableName = "%aa",
      emptyRes = true)

    check(
      """
        | SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS,
        | TYPE_CAT, TYPE_SCHEM, TYPE_NAME,
        | SELF_REFERENCING_COL_NAME, REF_GENERATION
        | FROM system.jdbc.tables
        | WHERE TABLE_CAT = 'ykf_catalog' AND TABLE_SCHEM LIKE '%aa' ESCAPE '\' AND
        | TABLE_TYPE IN ('ykf_type', 'ykf2_type')
        | ORDER BY TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME
        |""".stripMargin,
      catalog = "ykf_catalog",
      schema = "%aa",
      types = List("ykf_type", "ykf2_type"))

    check(
      """
        | SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS,
        | TYPE_CAT, TYPE_SCHEM, TYPE_NAME,
        | SELF_REFERENCING_COL_NAME, REF_GENERATION
        | FROM system.jdbc.tables
        | WHERE
        | TABLE_CAT = 'ykf_catalog' AND
        | TABLE_SCHEM LIKE '%aa' ESCAPE '\' AND
        | TABLE_NAME LIKE 'bb%' ESCAPE '\' AND
        | TABLE_TYPE IN ('ykf_type', 'ykf2_type')
        | ORDER BY TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME
        |""".stripMargin,
      catalog = "ykf_catalog",
      schema = "%aa",
      tableName = "bb%",
      types = List("ykf_type", "ykf2_type"))
  }

  test("Support GetColumns for Trino Fe") {
    def check(
        query: String,
        catalog: String = null,
        schema: String = null,
        tableName: String = null,
        colName: String = null): Unit = {
      parse(query) match {
        case GetColumns(catalogName, schemaPattern, tableNamePattern, colNamePattern) =>
          assert(catalog == catalogName)
          assert(schema == schemaPattern)
          assert(tableName == tableNamePattern)
          assert(colName == colNamePattern)
        case _ => fail(s"Query $query parse failed. ")
      }
    }

    check(
      """
        | SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
        | TYPE_NAME, COLUMN_SIZE, BUFFER_LENGTH, DECIMAL_DIGITS, NUM_PREC_RADIX,
        | NULLABLE, REMARKS, COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB,
        | CHAR_OCTET_LENGTH, ORDINAL_POSITION, IS_NULLABLE,
        | SCOPE_CATALOG, SCOPE_SCHEMA, SCOPE_TABLE,
        | SOURCE_DATA_TYPE, IS_AUTOINCREMENT, IS_GENERATEDCOLUMN
        | FROM system.jdbc.columns
        | ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION
        |""".stripMargin)

    check(
      """
        | SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
        | TYPE_NAME, COLUMN_SIZE, BUFFER_LENGTH, DECIMAL_DIGITS, NUM_PREC_RADIX,
        | NULLABLE, REMARKS, COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB,
        | CHAR_OCTET_LENGTH, ORDINAL_POSITION, IS_NULLABLE,
        | SCOPE_CATALOG, SCOPE_SCHEMA, SCOPE_TABLE,
        | SOURCE_DATA_TYPE, IS_AUTOINCREMENT, IS_GENERATEDCOLUMN
        | FROM system.jdbc.columns
        | WHERE TABLE_CAT IS NULL
        | ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION
        |""".stripMargin)

    check(
      """
        | SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
        | TYPE_NAME, COLUMN_SIZE, BUFFER_LENGTH, DECIMAL_DIGITS, NUM_PREC_RADIX,
        | NULLABLE, REMARKS, COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB,
        | CHAR_OCTET_LENGTH, ORDINAL_POSITION, IS_NULLABLE,
        | SCOPE_CATALOG, SCOPE_SCHEMA, SCOPE_TABLE,
        | SOURCE_DATA_TYPE, IS_AUTOINCREMENT, IS_GENERATEDCOLUMN
        | FROM system.jdbc.columns
        | WHERE TABLE_CAT = 'ykf_catalog'
        | ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION
        |""".stripMargin,
      catalog = "ykf_catalog")

    check(
      """
        | SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
        | TYPE_NAME, COLUMN_SIZE, BUFFER_LENGTH, DECIMAL_DIGITS, NUM_PREC_RADIX,
        | NULLABLE, REMARKS, COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB,
        | CHAR_OCTET_LENGTH, ORDINAL_POSITION, IS_NULLABLE,
        | SCOPE_CATALOG, SCOPE_SCHEMA, SCOPE_TABLE,
        | SOURCE_DATA_TYPE, IS_AUTOINCREMENT, IS_GENERATEDCOLUMN
        | FROM system.jdbc.columns
        | WHERE TABLE_CAT = 'ykf_catalog' AND TABLE_NAME LIKE '%aa' ESCAPE '\'
        | ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION
        |""".stripMargin,
      catalog = "ykf_catalog",
      tableName = "%aa")

    check(
      """
        | SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
        | TYPE_NAME, COLUMN_SIZE, BUFFER_LENGTH, DECIMAL_DIGITS, NUM_PREC_RADIX,
        | NULLABLE, REMARKS, COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB,
        | CHAR_OCTET_LENGTH, ORDINAL_POSITION, IS_NULLABLE,
        | SCOPE_CATALOG, SCOPE_SCHEMA, SCOPE_TABLE,
        | SOURCE_DATA_TYPE, IS_AUTOINCREMENT, IS_GENERATEDCOLUMN
        | FROM system.jdbc.columns
        | WHERE TABLE_CAT = 'ykf_catalog' AND TABLE_NAME LIKE '%aa' ESCAPE '\'
        | AND COLUMN_NAME  LIKE '%bb' ESCAPE '\'
        | ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION
        |""".stripMargin,
      catalog = "ykf_catalog",
      tableName = "%aa",
      colName = "%bb")

    check(
      """
        | SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
        | TYPE_NAME, COLUMN_SIZE, BUFFER_LENGTH, DECIMAL_DIGITS, NUM_PREC_RADIX,
        | NULLABLE, REMARKS, COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB,
        | CHAR_OCTET_LENGTH, ORDINAL_POSITION, IS_NULLABLE,
        | SCOPE_CATALOG, SCOPE_SCHEMA, SCOPE_TABLE,
        | SOURCE_DATA_TYPE, IS_AUTOINCREMENT, IS_GENERATEDCOLUMN
        | FROM system.jdbc.columns
        | WHERE TABLE_CAT = 'ykf_catalog'
        | AND TABLE_SCHEM  LIKE '%cc' ESCAPE '\'
        | AND TABLE_NAME LIKE '%aa' ESCAPE '\'
        | AND COLUMN_NAME  LIKE '%bb' ESCAPE '\'
        | ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION
        |""".stripMargin,
      catalog = "ykf_catalog",
      schema = "%cc",
      tableName = "%aa",
      colName = "%bb")
  }

  test("Support GetPrimaryKeys for Trino Fe") {
    val kyuubiTreeNode = parse(
      """
        | SELECT CAST(NULL AS varchar) TABLE_CAT,
        | CAST(NULL AS varchar) TABLE_SCHEM,
        | CAST(NULL AS varchar) TABLE_NAME,
        | CAST(NULL AS varchar) COLUMN_NAME,
        | CAST(NULL AS smallint) KEY_SEQ,
        | CAST(NULL AS varchar) PK_NAME
        | WHERE false
        |""".stripMargin)

    assert(kyuubiTreeNode.isInstanceOf[GetPrimaryKeys])
  }
}
