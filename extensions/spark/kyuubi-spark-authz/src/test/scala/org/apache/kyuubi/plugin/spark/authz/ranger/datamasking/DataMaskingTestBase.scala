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

package org.apache.kyuubi.plugin.spark.authz.ranger.datamasking

import java.sql.Timestamp

import scala.util.Try

// scalastyle:off
import org.apache.commons.codec.digest.DigestUtils.md5Hex
import org.apache.spark.sql.{Row, SparkSessionExtensions}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.RangerTestUsers._
import org.apache.kyuubi.plugin.spark.authz.SparkSessionProvider
import org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension

/**
 * Base trait for data masking tests, derivative classes shall name themselves following:
 *  DataMaskingFor CatalogImpl?  FileFormat? Additions? Suite
 */
trait DataMaskingTestBase extends AnyFunSuite with SparkSessionProvider with BeforeAndAfterAll {
// scalastyle:on
  override protected val extension: SparkSessionExtensions => Unit = new RangerSparkExtension

  private def setup(): Unit = {
    sql(s"CREATE TABLE IF NOT EXISTS default.src" +
      "(key int," +
      " value1 int," +
      " value2 string," +
      " value3 string," +
      " value4 timestamp," +
      " value5 string)" +
      s" $format")

    // NOTICE: `bob` has a row filter `key < 20`
    sql("INSERT INTO default.src " +
      "SELECT 1, 1, 'hello', 'world', timestamp'2018-11-17 12:34:56', 'World'")
    sql("INSERT INTO default.src " +
      "SELECT 20, 2, 'kyuubi', 'y', timestamp'2018-11-17 12:34:56', 'world'")
    sql("INSERT INTO default.src " +
      "SELECT 30, 3, 'spark', 'a', timestamp'2018-11-17 12:34:56', 'world'")

    // scalastyle:off
    val value1 = "hello WORD 123 ~!@# AßþΔЙקم๗ቐあア叶葉엽"
    val value2 = "AßþΔЙקم๗ቐあア叶葉엽 hello WORD 123 ~!@#"
    // AßþΔЙקم๗ቐあア叶葉엽 reference https://zh.wikipedia.org/zh-cn/Unicode#XML.E5.92.8CUnicode
    // scalastyle:on
    sql(s"INSERT INTO default.src " +
      s"SELECT 10, 4, '$value1', '$value1', timestamp'2018-11-17 12:34:56', '$value1'")
    sql("INSERT INTO default.src " +
      s"SELECT 11, 5, '$value2', '$value2', timestamp'2018-11-17 12:34:56', '$value2'")

    sql(s"CREATE TABLE default.unmasked $format AS SELECT * FROM default.src")
  }

  private def cleanup(): Unit = {
    sql("DROP TABLE IF EXISTS default.src")
    sql("DROP TABLE IF EXISTS default.unmasked")
  }

  override def beforeAll(): Unit = {
    doAs(admin, setup())
    super.beforeAll()
  }
  override def afterAll(): Unit = {
    doAs(admin, cleanup())
    spark.stop
    super.afterAll()
  }

  test("simple query with a user doesn't have mask rules") {
    checkAnswer(
      kent,
      "SELECT key FROM default.src order by key",
      Seq(Row(1), Row(10), Row(11), Row(20), Row(30)))
  }

  test("simple query with a user has mask rules") {
    val result =
      Seq(Row(md5Hex("1"), "xxxxx", "worlx", Timestamp.valueOf("2018-01-01 00:00:00"), "Xorld"))
    checkAnswer(
      bob,
      "SELECT value1, value2, value3, value4, value5 FROM default.src " +
        "where key = 1",
      result)
    checkAnswer(
      bob,
      "SELECT value1 as key, value2, value3, value4, value5 FROM default.src where key = 1",
      result)
  }

  test("star") {
    val result =
      Seq(Row(1, md5Hex("1"), "xxxxx", "worlx", Timestamp.valueOf("2018-01-01 00:00:00"), "Xorld"))
    checkAnswer(bob, "SELECT * FROM default.src where key = 1", result)
  }

  test("simple udf") {
    val result =
      Seq(Row(md5Hex("1"), "xxxxx", "worlx", Timestamp.valueOf("2018-01-01 00:00:00"), "Xorld"))
    checkAnswer(
      bob,
      "SELECT max(value1), max(value2), max(value3), max(value4), max(value5) FROM default.src" +
        " where key = 1",
      result)
  }

  test("complex udf") {
    val result =
      Seq(Row(md5Hex("1"), "xxxxx", "worlx", Timestamp.valueOf("2018-01-01 00:00:00"), "Xorld"))
    checkAnswer(
      bob,
      "SELECT coalesce(max(value1), 1), coalesce(max(value2), 1), coalesce(max(value3), 1), " +
        "coalesce(max(value4), timestamp '2018-01-01 22:33:44'), coalesce(max(value5), 1) " +
        "FROM default.src where key = 1",
      result)
  }

  test("in subquery") {
    val result =
      Seq(Row(md5Hex("1"), "xxxxx", "worlx", Timestamp.valueOf("2018-01-01 00:00:00"), "Xorld"))
    checkAnswer(
      bob,
      "SELECT value1, value2, value3, value4, value5 FROM default.src WHERE value2 in " +
        "(SELECT value2 as key FROM default.src where key = 1)",
      result)
  }

  test("create a unmasked table as select from a masked one") {
    withCleanTmpResources(Seq(("default.src2", "table"))) {
      doAs(
        bob,
        sql(s"CREATE TABLE default.src2 $format AS SELECT value1 FROM default.src " +
          s"where key = 1"))
      checkAnswer(bob, "SELECT value1 FROM default.src2", Seq(Row(md5Hex("1"))))
    }
  }

  test("insert into a unmasked table from a masked one") {
    withCleanTmpResources(Seq(("default.src2", "table"), ("default.src3", "table"))) {
      doAs(bob, sql(s"CREATE TABLE default.src2 (value1 string) $format"))
      doAs(
        bob,
        sql(s"INSERT INTO default.src2 SELECT value1 from default.src " +
          s"where key = 1"))
      doAs(
        bob,
        sql(s"INSERT INTO default.src2 SELECT value1 as v from default.src " +
          s"where key = 1"))
      checkAnswer(bob, "SELECT value1 FROM default.src2", Seq(Row(md5Hex("1")), Row(md5Hex("1"))))
      doAs(bob, sql(s"CREATE TABLE default.src3 (k int, value string) $format"))
      doAs(
        bob,
        sql(s"INSERT INTO default.src3 SELECT key, value1 from default.src  " +
          s"where key = 1"))
      doAs(
        bob,
        sql(s"INSERT INTO default.src3 SELECT key, value1 as v from default.src " +
          s"where key = 1"))
      checkAnswer(bob, "SELECT value FROM default.src3", Seq(Row(md5Hex("1")), Row(md5Hex("1"))))
    }
  }

  test("join on an unmasked table") {
    val s = "SELECT a.value1, b.value1 FROM default.src a" +
      " join default.unmasked b on a.value1=b.value1"
    checkAnswer(bob, s, Nil)
    checkAnswer(bob, s, Nil) // just for testing query multiple times, don't delete it
  }

  test("self join on a masked table") {
    val s = "SELECT a.value1, b.value1 FROM default.src a" +
      " join default.src b on a.value1=b.value1 where a.key = 1 and b.key = 1 "
    checkAnswer(bob, s, Seq(Row(md5Hex("1"), md5Hex("1"))))
    // just for testing query multiple times, don't delete it
    checkAnswer(bob, s, Seq(Row(md5Hex("1"), md5Hex("1"))))
  }

  test("self join on a masked table and filter the masked column with original value") {
    val s = "SELECT a.value1, b.value1 FROM default.src a" +
      " join default.src b on a.value1=b.value1" +
      " where a.value1='1' and b.value1='1'"
    checkAnswer(bob, s, Nil)
    checkAnswer(bob, s, Nil) // just for testing query multiple times, don't delete it
  }

  test("self join on a masked table and filter the masked column with masked value") {
    // scalastyle:off
    val s = "SELECT a.value1, b.value1 FROM default.src a" +
      " join default.src b on a.value1=b.value1" +
      s" where a.value1='${md5Hex("1")}' and b.value1='${md5Hex("1")}'"
    // TODO: The v1 an v2 relations generate different implicit type cast rules for filters
    // so the bellow test failed in derivative classes that us v2 data source, e.g., DataMaskingForIcebergSuite
    // For the issue itself, we might need check the spark logic first
    // DataMaskingStage1Marker Project [value1#178, value1#183]
    // +- Project [value1#178, value1#183]
    //   +- Filter ((cast(value1#178 as int) = cast(c4ca4238a0b923820dcc509a6f75849b as int)) AND (cast(value1#183 as int) = cast(c4ca4238a0b923820dcc509a6f75849b as int)))
    //      +- Join Inner, (value1#178 = value1#183)
    //         :- SubqueryAlias a
    //         :  +- SubqueryAlias testcat.default.src
    //         :     +- Filter (key#166 < 20)
    //         :        +- RowFilterMarker
    //         :           +- DataMaskingStage0Marker RelationV2[key#166, value1#167, value2#168, value3#169, value4#170, value5#171] default.src
    //         :              +- Project [key#166, md5(cast(cast(value1#167 as string) as binary)) AS value1#178, regexp_replace(regexp_replace(regexp_replace(value2#168, [A-Z], X, 1), [a-z], x, 1), [0-9], n, 1) AS value2#179, regexp_replace(regexp_replace(regexp_replace(value3#169, [A-Z], X, 5), [a-z], x, 5), [0-9], n, 5) AS value3#180, date_trunc(YEAR, value4#170, Some(Asia/Shanghai)) AS value4#181, concat(regexp_replace(regexp_replace(regexp_replace(left(value5#171, (length(value5#171) - 4)), [A-Z], X, 1), [a-z], x, 1), [0-9], n, 1), right(value5#171, 4)) AS value5#182]
    //         :                 +- RelationV2[key#166, value1#167, value2#168, value3#169, value4#170, value5#171] default.src
    //         +- SubqueryAlias b
    //            +- SubqueryAlias testcat.default.src
    //               +- Filter (key#172 < 20)
    //                  +- RowFilterMarker
    //                     +- DataMaskingStage0Marker RelationV2[key#172, value1#173, value2#174, value3#175, value4#176, value5#177] default.src
    //                        +- Project [key#172, md5(cast(cast(value1#173 as string) as binary)) AS value1#183, regexp_replace(regexp_replace(regexp_replace(value2#174, [A-Z], X, 1), [a-z], x, 1), [0-9], n, 1) AS value2#184, regexp_replace(regexp_replace(regexp_replace(value3#175, [A-Z], X, 5), [a-z], x, 5), [0-9], n, 5) AS value3#185, date_trunc(YEAR, value4#176, Some(Asia/Shanghai)) AS value4#186, concat(regexp_replace(regexp_replace(regexp_replace(left(value5#177, (length(value5#177) - 4)), [A-Z], X, 1), [a-z], x, 1), [0-9], n, 1), right(value5#177, 4)) AS value5#187]
    //                           +- RelationV2[key#172, value1#173, value2#174, value3#175, value4#176, value5#177] default.src
    //
    //
    // Project [value1#143, value1#148]
    // +- Filter ((value1#143 = c4ca4238a0b923820dcc509a6f75849b) AND (value1#148 = c4ca4238a0b923820dcc509a6f75849b))
    //   +- Join Inner, (value1#143 = value1#148)
    //      :- SubqueryAlias a
    //      :  +- SubqueryAlias spark_catalog.default.src
    //      :     +- Filter (key#60 < 20)
    //      :        +- RowFilterMarker
    //      :           +- DataMaskingStage0Marker Relation default.src[key#60,value1#61,value2#62,value3#63,value4#64,value5#65] parquet
    //      :              +- Project [key#60, md5(cast(cast(value1#61 as string) as binary)) AS value1#143, regexp_replace(regexp_replace(regexp_replace(value2#62, [A-Z], X, 1), [a-z], x, 1), [0-9], n, 1) AS value2#144, regexp_replace(regexp_replace(regexp_replace(value3#63, [A-Z], X, 5), [a-z], x, 5), [0-9], n, 5) AS value3#145, date_trunc(YEAR, value4#64, Some(Asia/Shanghai)) AS value4#146, concat(regexp_replace(regexp_replace(regexp_replace(left(value5#65, (length(value5#65) - 4)), [A-Z], X, 1), [a-z], x, 1), [0-9], n, 1), right(value5#65, 4)) AS value5#147]
    //      :                 +- Relation default.src[key#60,value1#61,value2#62,value3#63,value4#64,value5#65] parquet
    //      +- SubqueryAlias b
    //         +- SubqueryAlias spark_catalog.default.src
    //            +- Filter (key#153 < 20)
    //               +- RowFilterMarker
    //                  +- DataMaskingStage0Marker Relation default.src[key#60,value1#61,value2#62,value3#63,value4#64,value5#65] parquet
    //                     +- Project [key#153, md5(cast(cast(value1#154 as string) as binary)) AS value1#148, regexp_replace(regexp_replace(regexp_replace(value2#155, [A-Z], X, 1), [a-z], x, 1), [0-9], n, 1) AS value2#149, regexp_replace(regexp_replace(regexp_replace(value3#156, [A-Z], X, 5), [a-z], x, 5), [0-9], n, 5) AS value3#150, date_trunc(YEAR, value4#157, Some(Asia/Shanghai)) AS value4#151, concat(regexp_replace(regexp_replace(regexp_replace(left(value5#158, (length(value5#158) - 4)), [A-Z], X, 1), [a-z], x, 1), [0-9], n, 1), right(value5#158, 4)) AS value5#152]
    //                        +- Relation default.src[key#153,value1#154,value2#155,value3#156,value4#157,value5#158] parquet
    // checkAnswer(bob, s, Seq(Row(md5Hex("1"), md5Hex("1"))))
    //
    //
    // scalastyle:on

    // So here we use value2 to avoid type casting
    val s2 = "SELECT a.value1, b.value1 FROM default.src a" +
      " join default.src b on a.value1=b.value1" +
      s" where a.value2='xxxxx' and b.value2='xxxxx'"
    checkAnswer(bob, s2, Seq(Row(md5Hex("1"), md5Hex("1"))))
    // just for testing query multiple times, don't delete it
    checkAnswer(bob, s2, Seq(Row(md5Hex("1"), md5Hex("1"))))
  }

  test("union an unmasked table") {
    val s = """
      SELECT value1 from (
           SELECT a.value1 FROM default.src a where a.key = 1
           union
          (SELECT b.value1 FROM default.unmasked b)
      ) c order by value1
      """
    checkAnswer(bob, s, Seq(Row("1"), Row("2"), Row("3"), Row("4"), Row("5"), Row(md5Hex("1"))))
  }

  test("union a masked table") {
    val s = "SELECT a.value1 FROM default.src a where a.key = 1 union" +
      " (SELECT b.value1 FROM default.src b where b.key = 1)"
    checkAnswer(bob, s, Seq(Row(md5Hex("1"))))
  }

  test("KYUUBI #3581: permanent view should lookup rule on itself not the raw table") {
    val supported = doAs(
      permViewUser,
      Try(sql("CREATE OR REPLACE VIEW default.perm_view AS SELECT * FROM default.src")).isSuccess)
    assume(supported, s"view support for '$format' has not been implemented yet")

    withCleanTmpResources(Seq(("default.perm_view", "view"))) {
      checkAnswer(
        permViewUser,
        "SELECT value1, value2 FROM default.src where key = 1",
        Seq(Row(1, "hello")))
      checkAnswer(
        permViewUser,
        "SELECT value1, value2 FROM default.perm_view where key = 1",
        Seq(Row(md5Hex("1"), "hello")))
    }
  }

  // This test only includes a small subset of UCS-2 characters.
  // But in theory, it should work for all characters
  test("test MASK,MASK_SHOW_FIRST_4,MASK_SHOW_LAST_4 rule  with non-English character set") {
    val s1 = s"SELECT * FROM default.src where key = 10"
    val s2 = s"SELECT * FROM default.src where key = 11"
    // scalastyle:off
    checkAnswer(
      bob,
      s1,
      Seq(Row(
        10,
        md5Hex("4"),
        "xxxxxUXXXXUnnnUUUUUUXUUUUUUUUUUUUU",
        "hellxUXXXXUnnnUUUUUUXUUUUUUUUUUUUU",
        Timestamp.valueOf("2018-01-01 00:00:00"),
        "xxxxxUXXXXUnnnUUUUUUXUUUUUUUUUア叶葉엽")))
    checkAnswer(
      bob,
      s2,
      Seq(Row(
        11,
        md5Hex("5"),
        "XUUUUUUUUUUUUUUxxxxxUXXXXUnnnUUUUU",
        "AßþΔUUUUUUUUUUUxxxxxUXXXXUnnnUUUUU",
        Timestamp.valueOf("2018-01-01 00:00:00"),
        "XUUUUUUUUUUUUUUxxxxxUXXXXUnnnU~!@#")))
    // scalastyle:on
  }

  test("KYUUBI #5092: Spark crashes with ClassCastException when resolving a join") {
    import spark.sqlContext.implicits._
    doAs(
      "bob", {
        val df0 = spark.table("default.src")
          .select("key")
          .filter($"key" === 1)
          .sort($"key")
        assert(
          df0.as("a").join(
            right = df0.as("b"),
            joinExprs = $"a.key" === $"b.key",
            joinType = "left_outer").collect() === Seq(Row(1, 1)))
      })
  }
}
