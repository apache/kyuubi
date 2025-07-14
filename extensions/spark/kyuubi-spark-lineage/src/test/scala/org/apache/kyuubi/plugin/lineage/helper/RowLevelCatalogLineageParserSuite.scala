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

package org.apache.kyuubi.plugin.lineage.helper

import org.apache.kyuubi.plugin.lineage.Lineage
import org.apache.kyuubi.plugin.lineage.helper.SparkListenerHelper.SPARK_RUNTIME_VERSION

class RowLevelCatalogLineageParserSuite extends SparkSQLLineageParserHelperSuite {

  override def catalogName: String = {
    "org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTableCatalog"
  }

  test("columns lineage extract - WriteDelta") {
    assume(
      SPARK_RUNTIME_VERSION >= "3.5",
      "WriteDelta is only supported in SPARK_RUNTIME_VERSION >= 3.5")
    val ddls =
      """
        |create table v2_catalog.db.target_t(pk int not null, name string, price float)
        | TBLPROPERTIES ('supports-deltas'='true');
        |create table v2_catalog.db.source_t(pk int not null, name string, price float)
        | TBLPROPERTIES ('supports-deltas'='true');
        |create table v2_catalog.db.pivot_t(pk int not null, price float)
        | TBLPROPERTIES ('supports-deltas'='true')
        |""".stripMargin
    ddls.split(";").filter(_.nonEmpty).foreach(spark.sql(_).collect())

    withTable("v2_catalog.db.target_t", "v2_catalog.db.source_t", "v2_catalog.db.pivot_t") { _ =>
      val ret0 = extractLineageWithoutExecuting(
        "MERGE INTO v2_catalog.db.target_t AS target " +
          "USING v2_catalog.db.source_t AS source " +
          "ON target.pk = source.pk " +
          "WHEN MATCHED THEN " +
          "  UPDATE SET target.name = source.name, target.price = source.price " +
          "WHEN NOT MATCHED THEN " +
          "  INSERT (pk, name, price) VALUES (cast(source.pk as int), source.name, source.price)" +
          "WHEN NOT MATCHED BY SOURCE THEN delete")
      assert(ret0 == Lineage(
        List("v2_catalog.db.source_t", "v2_catalog.db.target_t"),
        List("v2_catalog.db.target_t"),
        List(
          (
            "v2_catalog.db.target_t.pk",
            Set("v2_catalog.db.source_t.pk", "v2_catalog.db.target_t.pk")),
          ("v2_catalog.db.target_t.name", Set("v2_catalog.db.source_t.name")),
          ("v2_catalog.db.target_t.price", Set("v2_catalog.db.source_t.price")))))

      val ret1 = extractLineageWithoutExecuting(
        "MERGE INTO v2_catalog.db.target_t AS target " +
          "USING v2_catalog.db.source_t AS source " +
          "ON target.pk = source.pk " +
          "WHEN MATCHED THEN " +
          "  UPDATE SET * " +
          "WHEN NOT MATCHED THEN " +
          "  INSERT *")
      assert(ret1 == Lineage(
        List("v2_catalog.db.source_t"),
        List("v2_catalog.db.target_t"),
        List(
          ("v2_catalog.db.target_t.pk", Set("v2_catalog.db.source_t.pk")),
          ("v2_catalog.db.target_t.name", Set("v2_catalog.db.source_t.name")),
          ("v2_catalog.db.target_t.price", Set("v2_catalog.db.source_t.price")))))

      val ret2 = extractLineageWithoutExecuting(
        "MERGE INTO v2_catalog.db.target_t AS target " +
          "USING (select a.pk, a.name, b.price " +
          "from v2_catalog.db.source_t a join " +
          "v2_catalog.db.pivot_t b) AS source " +
          "ON target.pk = source.pk " +
          "WHEN MATCHED THEN " +
          "  UPDATE SET * " +
          "WHEN NOT MATCHED THEN " +
          "  INSERT *")

      assert(ret2 == Lineage(
        List("v2_catalog.db.source_t", "v2_catalog.db.pivot_t"),
        List("v2_catalog.db.target_t"),
        List(
          ("v2_catalog.db.target_t.pk", Set("v2_catalog.db.source_t.pk")),
          ("v2_catalog.db.target_t.name", Set("v2_catalog.db.source_t.name")),
          ("v2_catalog.db.target_t.price", Set("v2_catalog.db.pivot_t.price")))))

      val ret3 = extractLineageWithoutExecuting(
        "update v2_catalog.db.target_t AS set name='abc' where price < 10 ")
      assert(ret3 == Lineage(
        List("v2_catalog.db.target_t"),
        List("v2_catalog.db.target_t"),
        List(
          ("v2_catalog.db.target_t.pk", Set("v2_catalog.db.target_t.pk")),
          ("v2_catalog.db.target_t.name", Set()),
          ("v2_catalog.db.target_t.price", Set("v2_catalog.db.target_t.price")))))
    }
  }

  test("columns lineage extract - ReplaceData") {
    assume(
      SPARK_RUNTIME_VERSION >= "3.5",
      "ReplaceData[SPARK-43963] for merge into is supported in SPARK_RUNTIME_VERSION >= 3.5")
    val ddls =
      """
        |create table v2_catalog.db.target_t(id int, name string, price float)
        |create table v2_catalog.db.source_t(id int, name string, price float)
        |create table v2_catalog.db.pivot_t(id int, price float)
        |""".stripMargin
    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withTable("v2_catalog.db.target_t", "v2_catalog.db.source_t", "v2_catalog.db.pivot_t") { _ =>
      val ret0 = extractLineageWithoutExecuting("MERGE INTO v2_catalog.db.target_t AS target " +
        "USING v2_catalog.db.source_t AS source " +
        "ON target.id = source.id " +
        "WHEN MATCHED THEN " +
        "  UPDATE SET target.name = source.name, target.price = source.price " +
        "WHEN NOT MATCHED THEN " +
        "  INSERT (id, name, price) VALUES (cast(source.id as int), source.name, source.price)")

      /**
       * The ReplaceData operation requires that target records which are read but do not match
       * any of the MATCHED or NOT MATCHED BY SOURCE clauses also be copied.
       * (refer to [[RewriteMergeIntoTable#buildReplaceDataMergeRowsPlan]])
       */
      assert(ret0 == Lineage(
        List("v2_catalog.db.source_t", "v2_catalog.db.target_t"),
        List("v2_catalog.db.target_t"),
        List(
          (
            "v2_catalog.db.target_t.id",
            Set("v2_catalog.db.source_t.id", "v2_catalog.db.target_t.id")),
          (
            "v2_catalog.db.target_t.name",
            Set("v2_catalog.db.source_t.name", "v2_catalog.db.target_t.name")),
          (
            "v2_catalog.db.target_t.price",
            Set("v2_catalog.db.source_t.price", "v2_catalog.db.target_t.price")))))

      val ret1 = extractLineageWithoutExecuting("MERGE INTO v2_catalog.db.target_t AS target " +
        "USING v2_catalog.db.source_t AS source " +
        "ON target.id = source.id " +
        "WHEN MATCHED THEN " +
        "  UPDATE SET * " +
        "WHEN NOT MATCHED THEN " +
        "  INSERT *")
      assert(ret1 == Lineage(
        List("v2_catalog.db.source_t", "v2_catalog.db.target_t"),
        List("v2_catalog.db.target_t"),
        List(
          (
            "v2_catalog.db.target_t.id",
            Set("v2_catalog.db.source_t.id", "v2_catalog.db.target_t.id")),
          (
            "v2_catalog.db.target_t.name",
            Set("v2_catalog.db.source_t.name", "v2_catalog.db.target_t.name")),
          (
            "v2_catalog.db.target_t.price",
            Set("v2_catalog.db.source_t.price", "v2_catalog.db.target_t.price")))))

      val ret2 = extractLineageWithoutExecuting("MERGE INTO v2_catalog.db.target_t AS target " +
        "USING (select a.id, a.name, b.price " +
        "from v2_catalog.db.source_t a join v2_catalog.db.pivot_t b) AS source " +
        "ON target.id = source.id " +
        "WHEN MATCHED THEN " +
        "  UPDATE SET * " +
        "WHEN NOT MATCHED THEN " +
        "  INSERT *")

      assert(ret2 == Lineage(
        List("v2_catalog.db.source_t", "v2_catalog.db.target_t", "v2_catalog.db.pivot_t"),
        List("v2_catalog.db.target_t"),
        List(
          (
            "v2_catalog.db.target_t.id",
            Set("v2_catalog.db.source_t.id", "v2_catalog.db.target_t.id")),
          (
            "v2_catalog.db.target_t.name",
            Set("v2_catalog.db.source_t.name", "v2_catalog.db.target_t.name")),
          (
            "v2_catalog.db.target_t.price",
            Set("v2_catalog.db.pivot_t.price", "v2_catalog.db.target_t.price")))))

      val ret3 = extractLineageWithoutExecuting(
        "update v2_catalog.db.target_t AS set name='abc' where price < 10 ")
      // For tables that do not support row-level deletion,
      // duplicate data of the same group may be included when writing.
      // plan is:
      // ReplaceData
      // +- Project [if ((price#1160 < cast(10 as float))) id#1158 else id#1158 AS id#1163,
      //    if ((price#1160 < cast(10 as float))) abc else name#1159 AS name#1164,
      //    if ((price#1160 < cast(10 as float))) price#1160 else price#1160 AS price#1165,
      //    _partition#1162]
      // +- RelationV2[id#1158, name#1159, price#1160, _partition#1162]
      //    v2_catalog.db.target_t v2_catalog.db.target_t
      assert(ret3 == Lineage(
        List("v2_catalog.db.target_t"),
        List("v2_catalog.db.target_t"),
        List(
          (
            "v2_catalog.db.target_t.id",
            Set("v2_catalog.db.target_t.price", "v2_catalog.db.target_t.id")),
          (
            "v2_catalog.db.target_t.name",
            Set("v2_catalog.db.target_t.price", "v2_catalog.db.target_t.name")),
          ("v2_catalog.db.target_t.price", Set("v2_catalog.db.target_t.price")))))
    }
  }
}
