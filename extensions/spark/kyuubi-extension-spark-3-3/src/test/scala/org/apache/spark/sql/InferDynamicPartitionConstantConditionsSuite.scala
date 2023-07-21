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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Literal

import org.apache.kyuubi.sql.InferDynamicPartitionConstantConditions

class InferDynamicPartitionConstantConditionsSuite extends KyuubiSparkSQLExtensionTest {

  test("infer dynamic partition constant conditions") {
    withTable("t1", "t2") {
      sql("CREATE TABLE t1 (c1 INT, p1 STRING) USING parquet PARTITIONED BY (p1)")
      sql("CREATE TABLE t2 (c2 INT, p2 STRING) USING parquet PARTITIONED BY (p2)")

      def check(query: String, expect: Map[String, Seq[Any]]): Unit = {
        val df = sql(query)
        val parts = df.queryExecution.analyzed.output.filter(o => expect.contains(o.name))
        val actualResult = InferDynamicPartitionConstantConditions.infer(
          parts,
          df.queryExecution.analyzed)
          .map {
            case (k, v) => k -> v.toSet
          }
        val expectResult = expect.map {
          case (k, v) =>
            val part = parts.find(_.name == k).get
            val values = v.map(Literal(_))
            part -> values.toSet
        }
        assert(actualResult == expectResult)
      }

      check("SELECT c1, p1 FROM t1 WHERE p1 = '1'", Map("p1" -> Seq("1")))

      check("SELECT c1, p1 FROM t1 WHERE p1 in ('1', '2')", Map("p1" -> Seq("1", "2")))

      check(
        "SELECT c1, p1 FROM t1 WHERE p1 in ('1', '2') and p1 in ('2', '3')",
        Map("p1" -> Seq("2")))

      check("SELECT c1, p1 FROM t1 WHERE p1 = '1' or p1 = '2'", Map("p1" -> Seq("1", "2")))

      check(
        "SELECT c1, p1 FROM t1 join t2 on t1.c1 = t2.c2 WHERE t1.p1 = '1'",
        Map("p1" -> Seq("1")))

      check(
        "SELECT c1, p1 FROM (select * from t1 WHERE p1 = '1') t join t2 on t.c1 = t2.c2",
        Map("p1" -> Seq("1")))

      check(
        """
          |SELECT c1, p1
          |FROM (SELECT c1, nvl(p1, 2) as p1 FROM t1 WHERE p1 = '1') t
          |JOIN t2 on t.c1 = t2.c2""".stripMargin,
        Map("p1" -> Seq("1")))

      check(
        """
          |SELECT c1, p1
          |FROM (SELECT c1, p1 FROM t1 WHERE p1 = '1'
          | UNION ALL SELECT c2, p2 FROM t2 WHERE p2 = '2') t
          |JOIN t2 on t.c1 = t2.c2""".stripMargin,
        Map("p1" -> Seq("1", "2")))

      check(
        """
          |SELECT c1, p1
          |FROM (SELECT c1, p1 FROM t1 WHERE p1 = '1'
          | UNION ALL SELECT c2, p2 FROM t2 WHERE p2 > '2') t
          |JOIN t2 on t.c1 = t2.c2""".stripMargin,
        Map())

      // generate from deterministic expressions
      check(
        """
          |SELECT c1, p1 FROM
          | (select c1, explode(from_json(p1, 'array<int>')) as p1 from t1 where p1 = '[1]')
          |""".stripMargin,
        Map("p1" -> Seq("[1]")))

    }
  }

}
