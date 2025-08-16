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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.command.{ResetCommand, SetCommand}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ReusedExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.internal.SQLConf

import org.apache.kyuubi.sql.KyuubiSQLConf._

/**
 * This rule split stage into two parts:
 *   1. previous stage
 *   2. final stage
 * For final stage, we can inject extra config. It's useful if we use repartition to optimize
 * small files that needs bigger shuffle partition size than previous.
 *
 * Let's say we have a query with 3 stages, then the logical machine like:
 *
 * Set/Reset Command -> cleanup previousStage config if user set the spark config.
 * Query -> AQE -> stage1 -> preparation (use previousStage to overwrite spark config)
 *       -> AQE -> stage2 -> preparation (use spark config)
 *       -> AQE -> stage3 -> preparation (use finalStage config to overwrite spark config,
 *                                        store spark config to previousStage.)
 *
 * An example of the new finalStage config:
 * `spark.sql.adaptive.advisoryPartitionSizeInBytes` ->
 * `spark.sql.finalStage.adaptive.advisoryPartitionSizeInBytes`
 */
case class FinalStageConfigIsolation(session: SparkSession) extends Rule[SparkPlan] {
  import FinalStageConfigIsolation._

  override def apply(plan: SparkPlan): SparkPlan = {
    // this rule has no meaning without AQE
    if (!conf.getConf(FINAL_STAGE_CONFIG_ISOLATION) ||
      !conf.getConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED)) {
      return plan
    }

    if (isFinalStage(plan)) {
      // We can not get the whole plan at query preparation phase to detect if current plan is
      // for writing, so we depend on a tag which is been injected at post resolution phase.
      // Note: we should still do clean up previous config for non-final stage to avoid such case:
      // the first statement is write, but the second statement is query.
      if (conf.getConf(FINAL_STAGE_CONFIG_ISOLATION_WRITE_ONLY) &&
        !WriteUtils.isWrite(session, plan)) {
        return plan
      }

      // set config for final stage
      session.conf.getAll.filter(_._1.startsWith(FINAL_STAGE_CONFIG_PREFIX)).foreach {
        case (k, v) =>
          val sparkConfigKey = s"spark.sql.${k.substring(FINAL_STAGE_CONFIG_PREFIX.length)}"
          val previousStageConfigKey =
            s"$PREVIOUS_STAGE_CONFIG_PREFIX${k.substring(FINAL_STAGE_CONFIG_PREFIX.length)}"
          // store the previous config only if we have not stored, to avoid some query only
          // have one stage that will overwrite real config.
          if (!session.sessionState.conf.contains(previousStageConfigKey)) {
            val originalValue =
              if (session.conf.getOption(sparkConfigKey).isDefined) {
                session.sessionState.conf.getConfString(sparkConfigKey)
              } else {
                // the default value of config is None, so we need to use a internal tag
                INTERNAL_UNSET_CONFIG_TAG
              }
            logInfo(s"Store config: $sparkConfigKey to previousStage, " +
              s"original value: $originalValue ")
            session.sessionState.conf.setConfString(previousStageConfigKey, originalValue)
          }
          logInfo(s"For final stage: set $sparkConfigKey = $v.")
          session.conf.set(sparkConfigKey, v)
      }
    } else {
      // reset config for previous stage
      session.conf.getAll.filter(_._1.startsWith(PREVIOUS_STAGE_CONFIG_PREFIX)).foreach {
        case (k, v) =>
          val sparkConfigKey = s"spark.sql.${k.substring(PREVIOUS_STAGE_CONFIG_PREFIX.length)}"
          logInfo(s"For previous stage: set $sparkConfigKey = $v.")
          if (v == INTERNAL_UNSET_CONFIG_TAG) {
            session.conf.unset(sparkConfigKey)
          } else {
            session.conf.set(sparkConfigKey, v)
          }
          // unset config so that we do not need to reset configs for every previous stage
          session.conf.unset(k)
      }
    }

    plan
  }

  /**
   * Currently formula depend on AQE in Spark 3.1.1, not sure it can work in future.
   */
  private def isFinalStage(plan: SparkPlan): Boolean = {
    var shuffleNum = 0
    var broadcastNum = 0
    var reusedNum = 0
    var queryStageNum = 0

    def collectNumber(p: SparkPlan): SparkPlan = {
      p transform {
        case shuffle: ShuffleExchangeLike =>
          shuffleNum += 1
          shuffle

        case broadcast: BroadcastExchangeLike =>
          broadcastNum += 1
          broadcast

        case reusedExchangeExec: ReusedExchangeExec =>
          reusedNum += 1
          reusedExchangeExec

        // query stage is leaf node so we need to transform it manually
        // compatible with Spark 3.5:
        // SPARK-42101: table cache is a independent query stage, so do not need include it.
        case queryStage: QueryStageExec if queryStage.nodeName != "TableCacheQueryStage" =>
          queryStageNum += 1
          collectNumber(queryStage.plan)
          queryStage
      }
    }
    collectNumber(plan)

    if (shuffleNum == 0) {
      // we don not care about broadcast stage here since it won't change partition number.
      true
    } else if (shuffleNum + broadcastNum + reusedNum == queryStageNum) {
      true
    } else {
      false
    }
  }
}
object FinalStageConfigIsolation {
  final val SQL_PREFIX = "spark.sql."
  final val FINAL_STAGE_CONFIG_PREFIX = "spark.sql.finalStage."
  final val PREVIOUS_STAGE_CONFIG_PREFIX = "spark.sql.previousStage."
  final val INTERNAL_UNSET_CONFIG_TAG = "__INTERNAL_UNSET_CONFIG_TAG__"

  def getPreviousStageConfigKey(configKey: String): Option[String] = {
    if (configKey.startsWith(SQL_PREFIX)) {
      Some(s"$PREVIOUS_STAGE_CONFIG_PREFIX${configKey.substring(SQL_PREFIX.length)}")
    } else {
      None
    }
  }
}

case class FinalStageConfigIsolationCleanRule(session: SparkSession) extends Rule[LogicalPlan] {
  import FinalStageConfigIsolation._

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case set @ SetCommand(Some((k, Some(_)))) if k.startsWith(SQL_PREFIX) =>
      checkAndUnsetPreviousStageConfig(k)
      set

    case reset @ ResetCommand(Some(k)) if k.startsWith(SQL_PREFIX) =>
      checkAndUnsetPreviousStageConfig(k)
      reset

    case other => other
  }

  private def checkAndUnsetPreviousStageConfig(configKey: String): Unit = {
    getPreviousStageConfigKey(configKey).foreach { previousStageConfigKey =>
      if (session.sessionState.conf.contains(previousStageConfigKey)) {
        logInfo(s"For previous stage: unset $previousStageConfigKey")
        session.conf.unset(previousStageConfigKey)
      }
    }
  }
}
