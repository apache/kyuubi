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
package org.apache.kyuubi.sql.observe

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, Crc32, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Sum}
import org.apache.spark.sql.catalyst.plans.logical.{CollectMetrics, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.types.{BinaryType, ByteType, DecimalType, IntegerType, LongType, ShortType, StringType}

import org.apache.kyuubi.sql.KyuubiSQLConf.INSERT_CHECKSUM_OBSERVER_AFTER_PROJECT_ENABLED
import org.apache.kyuubi.sql.observe.InsertChecksumObserverAfterProject._

case class InsertChecksumObserverAfterProject(session: SparkSession) extends Rule[LogicalPlan] {

  private val INSERT_COLLECT_METRICS_TAG = TreeNodeTag[Unit]("__INSERT_COLLECT_METRICS_TAG")

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.getConf(INSERT_CHECKSUM_OBSERVER_AFTER_PROJECT_ENABLED)) {
      plan resolveOperatorsUp {
        case p: Project if p.resolved && p.getTagValue(INSERT_COLLECT_METRICS_TAG).isEmpty =>
          val metricExprs = p.output.map(toChecksumExpr) :+ countExpr
          p.setTagValue(INSERT_COLLECT_METRICS_TAG, ())
          CollectMetrics(nextObserverName, metricExprs, p)
      }
    } else {
      plan
    }
  }

  private def toChecksumExpr(attr: Attribute): NamedExpression = {
    // sum(cast(crc32(cast(attr as binary)) as decimal(20, 0))) as attr_crc_sum
    Alias(
      Sum(Cast(Crc32(toBinaryExpr(attr)), LongDecimal)).toAggregateExpression(),
      attr.name + "_crc_sum")()
  }

  private def toBinaryExpr(attr: Attribute): Expression = {
    attr.dataType match {
      case BinaryType => attr
      case StringType | ByteType | ShortType | IntegerType | LongType => Cast(attr, BinaryType)
      case _ => Cast(Cast(attr, StringType), BinaryType)
    }
  }

  private def countExpr: NamedExpression = {
    Alias(Count(Literal(1)).toAggregateExpression(), "cnt")()
  }
}

object InsertChecksumObserverAfterProject {
  private val id = new AtomicLong(0)
  private def nextObserverName: String = s"CHECKSUM_OBSERVER_${id.getAndIncrement()}"
  private val LongDecimal = DecimalType(20, 0)
}
