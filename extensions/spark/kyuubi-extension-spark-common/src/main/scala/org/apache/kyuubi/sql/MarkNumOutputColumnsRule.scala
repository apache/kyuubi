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

package org.apache.kyuubi.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Union, V2WriteCommand}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand

case class MarkNumOutputColumnsRule(session: SparkSession)
  extends Rule[LogicalPlan] {
  import MarkNumOutputColumnsRule._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(KyuubiSQLConf.FINAL_STAGE_CONFIG_ISOLATION)) {
      return plan
    }
    if (session.conf.getOption(OUTPUT_NUM_COLUMNS).isDefined) {
      session.conf.unset(OUTPUT_NUM_COLUMNS)
    }

    def numOutputColumns(p: LogicalPlan): Option[Int] = p match {
      case w: DataWritingCommand => Some(w.outputColumnNames.size)
      case w: V2WriteCommand => Some(w.query.output.size)
      case u: Union if u.children.nonEmpty => numOutputColumns(u.children.head)
      case _ => None
    }
    val numCols = numOutputColumns(plan)
    numCols.foreach { n =>
      session.conf.set(OUTPUT_NUM_COLUMNS, s"$n")
    }
    plan
  }
}

object MarkNumOutputColumnsRule {
  val OUTPUT_NUM_COLUMNS = "spark.sql.internal.numOutputColumns"

  def numOutputColumns(session: SparkSession): Option[String] = {
    session.conf.getOption(OUTPUT_NUM_COLUMNS)
  }

  def isWrite(session: SparkSession, plan: SparkPlan): Boolean = {
    numOutputColumns(session).exists { num =>
      try {
        num.toInt == plan.output.size
      } catch {
        case _: Throwable => false
      }
    }
  }
}
