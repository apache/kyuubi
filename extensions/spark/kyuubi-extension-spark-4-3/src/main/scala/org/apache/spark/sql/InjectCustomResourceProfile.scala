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

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{CustomResourceProfileExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive._

import org.apache.kyuubi.sql.{KyuubiSQLConf, WriteUtils}

/**
 * Inject custom resource profile for final write stage, so we can specify custom
 * executor resource configs.
 */
case class InjectCustomResourceProfile(session: SparkSession)
  extends Rule[SparkPlan] with FinalRebalanceStageHelper {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(KyuubiSQLConf.FINAL_WRITE_STAGE_RESOURCE_ISOLATION_ENABLED)) {
      return plan
    }

    if (!WriteUtils.isWrite(session, plan)) {
      return plan
    }

    val stage = findFinalRebalanceStage(plan)
    if (stage.isEmpty) {
      return plan
    }

    // TODO: Ideally, We can call `CoarseGrainedSchedulerBackend.requestTotalExecutors` eagerly
    //   to reduce the task submit pending time, but it may lose task locality.
    //
    // By default, it would request executors when catch stage submit event.
    injectCustomResourceProfile(plan, stage.get.id)
  }

  private def injectCustomResourceProfile(plan: SparkPlan, id: Int): SparkPlan = {
    plan match {
      case stage: ShuffleQueryStageExec if stage.id == id =>
        CustomResourceProfileExec(stage)
      case _ => plan.mapChildren(child => injectCustomResourceProfile(child, id))
    }
  }
}
