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

package org.apache.kyuubi.engine.spark.operation

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.config.KyuubiConf.OperationModes.{ANALYZE, OperationMode, OPTIMIZE, PARSE}
import org.apache.kyuubi.engine.spark.{ArrayFetchIterator, IterableFetchIterator}
import org.apache.kyuubi.operation.OperationType
import org.apache.kyuubi.session.Session

/**
 * Perform the statement parsing, analyzing or optimizing only without executing it
 */
class PlanOnlyStatement(
    spark: SparkSession,
    session: Session,
    protected override val statement: String,
    mode: OperationMode)
  extends SparkOperation(spark, OperationType.EXECUTE_STATEMENT, session) {

  override protected def resultSchema: StructType = if (result == null) {
    new StructType().add("plan", "string")
  } else {
    result.schema
  }

  private def isSetOrReset(plan: LogicalPlan): Boolean = {
    val className = plan.getClass.getSimpleName
    className == "SetCommand" || className == "ResetCommand"
  }

  override protected def runInternal(): Unit = {
    try {
      val parsed = spark.sessionState.sqlParser.parsePlan(statement)
      parsed match {
        case cmd if isSetOrReset(cmd) =>
          result = spark.sql(statement)
          iter = new ArrayFetchIterator(result.collect())
        case otherPlan => mode match {
          case PARSE =>
            iter = new IterableFetchIterator(Seq(Row(otherPlan.toString())))
          case ANALYZE =>
            val analyzed = spark.sessionState.analyzer.execute(otherPlan)
            spark.sessionState.analyzer.checkAnalysis(analyzed)
            iter = new IterableFetchIterator(Seq(Row(analyzed.toString())))
          case OPTIMIZE =>
            val analyzed = spark.sessionState.analyzer.execute(otherPlan)
            spark.sessionState.analyzer.checkAnalysis(analyzed)
            val optimized = spark.sessionState.optimizer.execute(analyzed)
            iter = new IterableFetchIterator(Seq(Row(optimized.toString())))
        }
      }
    } catch {
      onError()
    }
  }
}
