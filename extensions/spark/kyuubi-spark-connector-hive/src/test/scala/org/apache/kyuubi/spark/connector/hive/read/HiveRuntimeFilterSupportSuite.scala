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

package org.apache.kyuubi.spark.connector.hive.read

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, InSet}
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, Expressions}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.types._

class HiveRuntimeFilterSupportSuite extends SparkFunSuite {

  private val partitionSchema = StructType(Seq(
    StructField("dt", StringType),
    StructField("id", LongType)))

  // Build a V2 `IN` predicate with a single column ref followed by literals.
  private def inPredicate(column: String, values: AnyRef*): Predicate = {
    val children: Array[V2Expression] =
      (Expressions.column(column) +: values.map(Expressions.literal[AnyRef])).toArray
    new Predicate("IN", children)
  }

  test("filterAttributes returns one NamedReference per partition column") {
    val refs = HiveRuntimeFilterSupport.filterAttributes(Seq("dt", "id"))
    assert(refs.length == 2)
    assert(refs.map(_.fieldNames().toSeq) === Array(Seq("dt"), Seq("id")))
  }

  test("filterAttributes returns empty array when no partition columns") {
    assert(HiveRuntimeFilterSupport.filterAttributes(Seq.empty).isEmpty)
  }

  test("toCatalystPartitionFilters returns Nil for empty predicate array") {
    val out = HiveRuntimeFilterSupport.toCatalystPartitionFilters(
      Array.empty[Predicate], partitionSchema, isCaseSensitive = false)
    assert(out.isEmpty)
  }

  test("IN against a partition column is translated to catalyst InSet") {
    val out = HiveRuntimeFilterSupport.toCatalystPartitionFilters(
      Array(inPredicate("dt", "2026-01-01", "2026-05-01")),
      partitionSchema,
      isCaseSensitive = false)

    assert(out.size == 1)
    val inSet = out.head.asInstanceOf[InSet]
    val attr = inSet.child.asInstanceOf[AttributeReference]
    assert(attr.name == "dt")
    assert(attr.dataType == StringType)
    assert(inSet.hset.map(_.toString) === Set("2026-01-01", "2026-05-01"))
  }

  test("IN against a non-partition column is dropped as a whole") {
    val out = HiveRuntimeFilterSupport.toCatalystPartitionFilters(
      Array(inPredicate("non_partition_col", "x")),
      partitionSchema,
      isCaseSensitive = false)
    assert(out.isEmpty)
  }

  test("IN with mismatched literal dataType is dropped as a whole") {
    val out = HiveRuntimeFilterSupport.toCatalystPartitionFilters(
      Array(inPredicate("dt", java.lang.Long.valueOf(123L), java.lang.Long.valueOf(456L))),
      partitionSchema,
      isCaseSensitive = false)
    assert(out.isEmpty)
  }

  test("IN with case-different column name is accepted in case-insensitive mode") {
    val out = HiveRuntimeFilterSupport.toCatalystPartitionFilters(
      Array(inPredicate("DT", "2026-01-01")),
      partitionSchema,
      isCaseSensitive = false)
    assert(out.size == 1)
    assert(out.head.asInstanceOf[InSet].child.asInstanceOf[AttributeReference].name == "dt")
  }

  test("IN with case-different column name is rejected in case-sensitive mode") {
    val out = HiveRuntimeFilterSupport.toCatalystPartitionFilters(
      Array(inPredicate("DT", "2026-01-01")),
      partitionSchema,
      isCaseSensitive = true)
    assert(out.isEmpty)
  }

  test("non-IN predicate is ignored") {
    val eq = new Predicate(
      "=",
      Array[V2Expression](Expressions.column("dt"), Expressions.literal[AnyRef]("x")))
    val out = HiveRuntimeFilterSupport.toCatalystPartitionFilters(
      Array(eq), partitionSchema, isCaseSensitive = false)
    assert(out.isEmpty)
  }

  test("mixed predicate list keeps the accepted ones and drops the rest") {
    val good = inPredicate("dt", "2026-05-01")
    val bad = inPredicate("non_partition_col", "x")
    val out = HiveRuntimeFilterSupport.toCatalystPartitionFilters(
      Array(good, bad), partitionSchema, isCaseSensitive = false)
    assert(out.size == 1)
    assert(out.head.asInstanceOf[InSet].child.asInstanceOf[AttributeReference].name == "dt")
  }
}
