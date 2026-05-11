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
package org.apache.kyuubi.engine.jdbc

import java.sql.ResultSet

import scala.collection.mutable.ArrayBuffer

object MetadataTestHelpers {

  def collectCol(rs: ResultSet, columnLabel: String): Set[String] =
    try {
      val out = ArrayBuffer[String]()
      while (rs.next()) out += rs.getString(columnLabel)
      out.toSet
    } finally rs.close()

  def rowCount(rs: ResultSet): Int =
    try {
      var n = 0
      while (rs.next()) n += 1
      n
    } finally rs.close()

  def drain(rs: ResultSet): Unit =
    try while (rs.next()) {}
    finally rs.close()

  case class ExpectedColumn(
      name: String,
      sqlType: Option[Int] = None,
      nullable: Option[Boolean] = None)

  // Asserts target row is present AND every returned row honors the supplied filters
  // (catches "filter param silently dropped" regressions). Does not close rs.
  def assertTableRow(
      rs: ResultSet,
      tableName: String,
      tableCatalog: Option[String] = None,
      tableSchema: Option[String] = None,
      tableType: Option[String] = None): Unit = {
    case class Row(catalog: String, schema: String, name: String, tType: String)
    val seen = ArrayBuffer[Row]()
    while (rs.next()) {
      seen += Row(
        rs.getString("TABLE_CAT"),
        rs.getString("TABLE_SCHEM"),
        rs.getString("TABLE_NAME"),
        rs.getString("TABLE_TYPE"))
    }
    def matches(r: Row): Boolean = {
      val nm = r.name != null && r.name.equalsIgnoreCase(tableName)
      val cat = tableCatalog.forall(c => r.catalog != null && r.catalog.equalsIgnoreCase(c))
      val sch = tableSchema.forall(s => r.schema != null && r.schema.equalsIgnoreCase(s))
      val tt = tableType.forall(t => r.tType != null && r.tType.equalsIgnoreCase(t))
      nm && cat && sch && tt
    }
    val matched = seen.count(matches)
    assert(
      matched >= 1,
      s"expected at least one row matching tableName=$tableName " +
        s"catalog=$tableCatalog schema=$tableSchema type=$tableType, got rows=$seen")
    val stray = seen.filterNot(matches)
    assert(
      stray.isEmpty,
      s"expected ALL rows to honor filter tableName=$tableName " +
        s"catalog=$tableCatalog schema=$tableSchema type=$tableType " +
        s"(filter param appears to have been ignored), stray rows=$stray")
  }

  // Does not consume or close rs.
  def assertJdbcSpecColumnNames(rs: ResultSet, required: Set[String]): Unit = {
    val md = rs.getMetaData
    val present = (1 to md.getColumnCount).map(i => md.getColumnLabel(i).toUpperCase).toSet
    val missing = required.map(_.toUpperCase) -- present
    assert(
      missing.isEmpty,
      s"missing JDBC spec columns: $missing; present columns: $present")
  }

  // Asserts (sorted by ORDINAL_POSITION) COLUMN_NAME + optional DATA_TYPE / IS_NULLABLE per
  // ExpectedColumn, and that every row honors the catalog/schema/table filter. Closes rs.
  // Pass tableCatalog for catalog-as-database drivers (MySQL family, ClickHouse) and
  // tableSchema for schema-as-database drivers (PG, Oracle, Phoenix, Impala).
  def assertColumnContract(
      rs: ResultSet,
      tableName: String,
      expected: Seq[ExpectedColumn],
      tableCatalog: Option[String] = None,
      tableSchema: Option[String] = None): Unit = {
    case class Row(
        catalog: String,
        schema: String,
        table: String,
        name: String,
        ordinal: Int,
        dataType: Int,
        isNullable: String)
    val all = ArrayBuffer[Row]()
    try {
      while (rs.next()) {
        all += Row(
          rs.getString("TABLE_CAT"),
          rs.getString("TABLE_SCHEM"),
          rs.getString("TABLE_NAME"),
          rs.getString("COLUMN_NAME"),
          rs.getInt("ORDINAL_POSITION"),
          rs.getInt("DATA_TYPE"),
          Option(rs.getString("IS_NULLABLE")).getOrElse(""))
      }
    } finally rs.close()

    def catalogMatches(r: Row): Boolean = tableCatalog match {
      case Some(c) => r.catalog != null && r.catalog.equalsIgnoreCase(c)
      case None => true
    }
    def schemaMatches(r: Row): Boolean = tableSchema match {
      case Some(s) => r.schema != null && r.schema.equalsIgnoreCase(s)
      case None => true
    }
    def tableMatches(r: Row): Boolean =
      r.table != null && r.table.equalsIgnoreCase(tableName)

    val stray = all.filterNot(r => catalogMatches(r) && schemaMatches(r) && tableMatches(r))
    assert(
      stray.isEmpty,
      s"expected ALL rows to honor filter tableName=$tableName " +
        s"catalog=$tableCatalog schema=$tableSchema " +
        s"(filter param appears to have been ignored), stray rows=$stray")

    val sorted = all.sortBy(_.ordinal)
    assert(
      sorted.size == expected.size,
      s"column count mismatch for $tableName: expected ${expected.size}, got ${sorted.size}; " +
        s"rows = $sorted")
    sorted.zip(expected).zipWithIndex.foreach { case ((actual, exp), idx) =>
      assert(
        actual.name.equalsIgnoreCase(exp.name),
        s"$tableName col[$idx] name: expected ${exp.name}, got ${actual.name}")
      exp.sqlType.foreach { t =>
        assert(
          actual.dataType == t,
          s"$tableName col[${exp.name}] DATA_TYPE: expected $t, got ${actual.dataType}")
      }
      exp.nullable.foreach { n =>
        val expectedFlag = if (n) "YES" else "NO"
        assert(
          actual.isNullable.equalsIgnoreCase(expectedFlag),
          s"$tableName col[${exp.name}] IS_NULLABLE: expected $expectedFlag, " +
            s"got ${actual.isNullable}")
      }
    }
  }
}
