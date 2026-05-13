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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, In, Literal}
import org.apache.spark.sql.sources.{EqualTo, Filter, In => FilterIn}
import org.apache.spark.sql.types._

class HiveRuntimeFilterSupportSuite extends SparkFunSuite {

  private val partitionSchema = StructType(Seq(
    StructField("dt", StringType),
    StructField("id", LongType)))

  test("filterAttributes returns one NamedReference per partition column") {
    val refs = HiveRuntimeFilterSupport.filterAttributes(Seq("dt", "id"))
    assert(refs.length == 2)
    assert(refs.map(_.fieldNames().toSeq) === Array(Seq("dt"), Seq("id")))
  }

  test("filterAttributes returns empty array when no partition columns") {
    assert(HiveRuntimeFilterSupport.filterAttributes(Seq.empty).isEmpty)
  }

  test("toCatalystPartitionFilters returns Nil for empty filter array") {
    val out = HiveRuntimeFilterSupport.toCatalystPartitionFilters(
      Array.empty[Filter],
      partitionSchema,
      isCaseSensitive = false)
    assert(out.isEmpty)
  }

  test("IN against a partition column is translated to catalyst In") {
    val out = HiveRuntimeFilterSupport.toCatalystPartitionFilters(
      Array[Filter](FilterIn("dt", Array[Any]("2026-01-01", "2026-05-01"))),
      partitionSchema,
      isCaseSensitive = false)

    assert(out.size == 1)
    val in = out.head.asInstanceOf[In]
    val attr = in.value.asInstanceOf[AttributeReference]
    assert(attr.name == "dt")
    assert(attr.dataType == StringType)
    val literalValues = in.list.map(_.asInstanceOf[Literal].value.toString)
    assert(literalValues === Seq("2026-01-01", "2026-05-01"))
  }

  test("IN against a non-partition column is dropped as a whole") {
    val out = HiveRuntimeFilterSupport.toCatalystPartitionFilters(
      Array[Filter](FilterIn("non_partition_col", Array[Any]("x"))),
      partitionSchema,
      isCaseSensitive = false)
    assert(out.isEmpty)
  }

  test("IN with case-different column name is accepted in case-insensitive mode") {
    val out = HiveRuntimeFilterSupport.toCatalystPartitionFilters(
      Array[Filter](FilterIn("DT", Array[Any]("2026-01-01"))),
      partitionSchema,
      isCaseSensitive = false)
    assert(out.size == 1)
    assert(out.head.asInstanceOf[In].value.asInstanceOf[AttributeReference].name == "dt")
  }

  test("IN with case-different column name is rejected in case-sensitive mode") {
    val out = HiveRuntimeFilterSupport.toCatalystPartitionFilters(
      Array[Filter](FilterIn("DT", Array[Any]("2026-01-01"))),
      partitionSchema,
      isCaseSensitive = true)
    assert(out.isEmpty)
  }

  test("non-IN filter is ignored") {
    val out = HiveRuntimeFilterSupport.toCatalystPartitionFilters(
      Array[Filter](EqualTo("dt", "x")),
      partitionSchema,
      isCaseSensitive = false)
    assert(out.isEmpty)
  }

  test("mixed filter list keeps the accepted ones and drops the rest") {
    val good = FilterIn("dt", Array[Any]("2026-05-01"))
    val bad = FilterIn("non_partition_col", Array[Any]("x"))
    val out = HiveRuntimeFilterSupport.toCatalystPartitionFilters(
      Array[Filter](good, bad),
      partitionSchema,
      isCaseSensitive = false)
    assert(out.size == 1)
    assert(out.head.asInstanceOf[In].value.asInstanceOf[AttributeReference].name == "dt")
  }
}
