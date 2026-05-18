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

import java.util.Locale

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, In, Literal}
import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference}
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.StructTypeHelper
import org.apache.spark.sql.sources.{Filter, In => FilterIn}
import org.apache.spark.sql.types.StructType

/**
 * Helpers for a Hive-backed V2 [[org.apache.spark.sql.connector.read.Scan]] to
 * implement [[org.apache.spark.sql.connector.read.SupportsRuntimeFiltering]]
 * for Dynamic Partition Pruning (DPP).
 *
 * Spark's `DataSourceV2Strategy` currently only emits the `IN` form as a DPP
 * runtime filter, so translation here handles `In` only. Any filter whose
 * attribute does not match a known partition column is dropped; drops are
 * logged at DEBUG.
 *
 * We deliberately use the V1 `SupportsRuntimeFiltering` instead of the newer
 * `SupportsRuntimeV2Filtering` to keep this connector compilable against
 * Spark 3.3, where `SupportsRuntimeV2Filtering` was introduced in Spark 3.4.
 */
object HiveRuntimeFilterSupport extends Logging {

  /**
   * Build the runtime-filterable attribute array. Only partition columns are exposed
   * because DPP is only beneficial at the partition directory granularity.
   */
  def filterAttributes(partitionColumnNames: Seq[String]): Array[NamedReference] = {
    partitionColumnNames.map(Expressions.column).toArray
  }

  /**
   * Translate Spark's runtime V1 `In` filters into catalyst [[In]] expressions
   * bound to the given partition attributes.
   *
   * A filter is accepted only when it is a [[FilterIn]] whose attribute resolves
   * to a known partition column.
   */
  def toCatalystPartitionFilters(
      filters: Array[Filter],
      partitionSchema: StructType,
      isCaseSensitive: Boolean): Seq[Expression] = {
    val attrByName: Map[String, AttributeReference] =
      partitionSchema.toAttributes
        .map(a => normalize(a.name, isCaseSensitive) -> a).toMap

    val accepted = filters.toSeq.flatMap {
      case FilterIn(attribute, values) =>
        attrByName.get(normalize(attribute, isCaseSensitive)).map { attr =>
          In(attr, values.toSeq.map(v => Literal.create(v, attr.dataType)))
        }
      case _ => None
    }

    if (accepted.length < filters.length) {
      logDebug(
        s"Dropped ${filters.length - accepted.length} of ${filters.length} runtime " +
          s"filter(s) not applicable to partition columns " +
          s"[${partitionSchema.fieldNames.mkString(", ")}]")
    }
    accepted
  }

  private def normalize(name: String, isCaseSensitive: Boolean): String =
    if (isCaseSensitive) {
      name
    } else {
      name.toLowerCase(Locale.ROOT)
    }
}
