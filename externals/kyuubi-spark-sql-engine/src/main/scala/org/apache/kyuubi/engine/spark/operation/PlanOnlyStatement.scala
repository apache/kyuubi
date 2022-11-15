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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf.{OPERATION_PLAN_ONLY_EXCLUDES, OPERATION_PLAN_ONLY_OUT_STYLE}
import org.apache.kyuubi.operation.{AnalyzeMode, ArrayFetchIterator, ExecutionMode, IterableFetchIterator, JsonStyle, OptimizeMode, OptimizeWithStatsMode, ParseMode, PhysicalMode, PlainStyle, PlanOnlyMode, PlanOnlyStyle, UnknownMode, UnknownStyle}
import org.apache.kyuubi.operation.PlanOnlyMode.{notSupportedModeError, unknownModeError}
import org.apache.kyuubi.operation.PlanOnlyStyle.{notSupportedStyleError, unknownStyleError}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

/**
 * Perform the statement parsing, analyzing or optimizing only without executing it
 */
class PlanOnlyStatement(
    session: Session,
    override val statement: String,
    mode: PlanOnlyMode)
  extends SparkOperation(session) {

  private val operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)
  private val planExcludes: Seq[String] = {
    spark.conf.getOption(OPERATION_PLAN_ONLY_EXCLUDES.key).map(_.split(",").map(_.trim).toSeq)
      .getOrElse(session.sessionManager.getConf.get(OPERATION_PLAN_ONLY_EXCLUDES))
  }

  private val style = PlanOnlyStyle.fromString(spark.conf.get(
    OPERATION_PLAN_ONLY_OUT_STYLE.key,
    session.sessionManager.getConf.get(OPERATION_PLAN_ONLY_OUT_STYLE)))
  spark.conf.set(OPERATION_PLAN_ONLY_OUT_STYLE.key, style.name)

  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  override protected def resultSchema: StructType = {
    if (result == null) {
      new StructType().add("plan", "string")
    } else if (result.isEmpty) {
      new StructType().add("result", "string")
    } else result.schema
  }

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(operationLog)
    super.beforeRun()
  }

  override protected def runInternal(): Unit = withLocalProperties {
    try {
      SQLConf.withExistingConf(spark.sessionState.conf) {
        val parsed = spark.sessionState.sqlParser.parsePlan(statement)

        parsed match {
          case cmd if planExcludes.contains(cmd.getClass.getSimpleName) =>
            result = spark.sql(statement)
            iter = new ArrayFetchIterator(result.collect())

          case plan => style match {
              case PlainStyle => explainWithPlainStyle(plan)
              case JsonStyle => explainWithJsonStyle(plan)
              case UnknownStyle => unknownStyleError(style)
              case other => throw notSupportedStyleError(other, "Spark SQL")
            }
        }
      }
    } catch {
      onError()
    }
  }

  private def explainWithPlainStyle(plan: LogicalPlan): Unit = {
    mode match {
      case ParseMode =>
        iter = new IterableFetchIterator(Seq(Row(plan.toString())))
      case AnalyzeMode =>
        val analyzed = spark.sessionState.analyzer.execute(plan)
        spark.sessionState.analyzer.checkAnalysis(analyzed)
        iter = new IterableFetchIterator(Seq(Row(analyzed.toString())))
      case OptimizeMode =>
        val analyzed = spark.sessionState.analyzer.execute(plan)
        spark.sessionState.analyzer.checkAnalysis(analyzed)
        val optimized = spark.sessionState.optimizer.execute(analyzed)
        iter = new IterableFetchIterator(Seq(Row(optimized.toString())))
      case OptimizeWithStatsMode =>
        val analyzed = spark.sessionState.analyzer.execute(plan)
        spark.sessionState.analyzer.checkAnalysis(analyzed)
        val optimized = spark.sessionState.optimizer.execute(analyzed)
        optimized.stats
        iter = new IterableFetchIterator(Seq(Row(optimized.treeString(
          verbose = true,
          addSuffix = true,
          SQLConf.get.maxToStringFields,
          printOperatorId = false))))
      case PhysicalMode =>
        val physical = spark.sql(statement).queryExecution.sparkPlan
        iter = new IterableFetchIterator(Seq(Row(physical.toString())))
      case ExecutionMode =>
        val executed = spark.sql(statement).queryExecution.executedPlan
        iter = new IterableFetchIterator(Seq(Row(executed.toString())))
      case UnknownMode => throw unknownModeError(mode)
      case _ => throw notSupportedModeError(mode, "Spark SQL")
    }
  }

  private def explainWithJsonStyle(plan: LogicalPlan): Unit = {
    mode match {
      case ParseMode =>
        iter = new IterableFetchIterator(Seq(Row(plan.toJSON)))
      case AnalyzeMode =>
        val analyzed = spark.sessionState.analyzer.execute(plan)
        spark.sessionState.analyzer.checkAnalysis(analyzed)
        iter = new IterableFetchIterator(Seq(Row(analyzed.toJSON)))
      case OptimizeMode | OptimizeWithStatsMode =>
        val analyzed = spark.sessionState.analyzer.execute(plan)
        spark.sessionState.analyzer.checkAnalysis(analyzed)
        val optimized = spark.sessionState.optimizer.execute(analyzed)
        iter = new IterableFetchIterator(Seq(Row(optimized.toJSON)))
      case PhysicalMode =>
        val physical = spark.sql(statement).queryExecution.sparkPlan
        iter = new IterableFetchIterator(Seq(Row(physical.toJSON)))
      case ExecutionMode =>
        val executed = spark.sql(statement).queryExecution.executedPlan
        iter = new IterableFetchIterator(Seq(Row(executed.toJSON)))
      case UnknownMode => throw unknownModeError(mode)
      case _ =>
        throw KyuubiSQLException(s"The operation mode $mode" +
          " doesn't support in Spark SQL engine.")
    }
  }
}
