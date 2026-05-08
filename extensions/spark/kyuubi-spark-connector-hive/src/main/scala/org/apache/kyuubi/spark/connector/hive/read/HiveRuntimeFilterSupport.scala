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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, InSet}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.expressions.{Expressions, Literal => V2Literal, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.types.StructType

/**
 * Helpers for a Hive-backed V2 [[org.apache.spark.sql.connector.read.Scan]] to
 * implement [[org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering]]
 * for Dynamic Partition Pruning (DPP).
 *
 * Spark's `DataSourceV2Strategy` currently only emits the `IN` form of V2
 * [[Predicate]] as a DPP runtime filter, so translation here handles `IN` only.
 * Any predicate that does not match the expected shape (single partition column
 * ref + scalar literals with matching dataType) is dropped as a whole to
 * avoid incorrect pruning; drops are logged at DEBUG.
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
   * Translate Spark's runtime V2 `IN` predicates into catalyst `InSet(attr, Set[Any])`
   * expressions bound to the given partition attributes.
   */
  def toCatalystPartitionFilters(
      predicates: Array[Predicate],
      partitionSchema: StructType,
      isCaseSensitive: Boolean): Seq[Expression] = {
    val attrByName: Map[String, AttributeReference] =
      DataTypeUtils.toAttributes(partitionSchema)
        .map(a => normalize(a.name, isCaseSensitive) -> a).toMap

    val accepted = predicates.toSeq.flatMap(p => convertIn(p, attrByName, isCaseSensitive))
    if (accepted.length < predicates.length) {
      logDebug(
        s"Dropped ${predicates.length - accepted.length} of ${predicates.length} runtime " +
          s"filter(s) not applicable to partition columns " +
          s"[${partitionSchema.fieldNames.mkString(", ")}]")
    }
    accepted
  }

  /**
   * Convert a single V2 `IN` predicate into a catalyst [[InSet]], or `None` if it cannot
   * be safely converted. A predicate is accepted only when its first child is a
   * [[NamedReference]] resolving to a known partition column and every remaining child
   * is a scalar V2 [[V2Literal]] whose dataType matches the partition column's dataType.
   */
  private def convertIn(
      predicate: Predicate,
      attrByName: Map[String, AttributeReference],
      isCaseSensitive: Boolean): Option[InSet] = {
    val children = predicate.children()
    if (predicate.name() != "IN" || children.length < 2) {
      None
    } else {
      children.head match {
        case ref: NamedReference =>
          val colName = normalize(ref.fieldNames().mkString("."), isCaseSensitive)
          attrByName.get(colName).flatMap { attr =>
            val values = children.tail
            val allLiteralsMatch = values.forall {
              case lit: V2Literal[_] => DataTypeUtils.sameType(lit.dataType(), attr.dataType)
              case _ => false
            }
            if (allLiteralsMatch) {
              val literalValues = values.iterator.map {
                case lit: V2Literal[_] => lit.value()
              }.toSet[Any]
              Some(InSet(attr, literalValues))
            } else {
              None
            }
          }
        case _ => None
      }
    }
  }

  private def normalize(name: String, isCaseSensitive: Boolean): String =
    if (isCaseSensitive) {
      name
    } else {
      name.toLowerCase(Locale.ROOT)
    }
}
