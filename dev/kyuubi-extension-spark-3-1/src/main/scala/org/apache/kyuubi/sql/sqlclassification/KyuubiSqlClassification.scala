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

package org.apache.kyuubi.sql.sqlclassification

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

import org.apache.kyuubi.sql.KyuubiSQLConf._

case class KyuubiSqlClassification(session: SparkSession) extends Rule[LogicalPlan] {

  import KyuubiSqlConf._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (CLASSIFICATION_ENABLED && plan.resolved && !plan.analyzed) {
      val simpleName = plan.getClass.getSimpleName
      val sqlClassification = KyuubiGetSqlClassification.getSqlClassification(simpleName)
      session.conf.set(SPARK_SQL_CLASSIFICATION, sqlClassification)
    }
    plan
  }
}

object KyuubiSqlConf {

  val CLASSIFICATION_ENABLED = SQLConf.get.getConf(SQL_CLASSIFICATION_ENABLED)
  val SPARK_SQL_CLASSIFICATION = "spark.sql.classification"
}
