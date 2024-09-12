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

package org.apache.kyuubi.sql.compact

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

case class CompactTableSparkStrategy(sparkSession: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case smallFileCollect @ SmallFileCollect(
            LogicalRelation(hadoopFsRelation: HadoopFsRelation, _, Some(catalogTable), _),
            targetSizeInBytes) =>
        SmallFileCollectExec(
          hadoopFsRelation,
          smallFileCollect.output,
          catalogTable,
          targetSizeInBytes) :: Nil
      case SmallFileMerge(child, noMerge) if !noMerge =>
        SmallFileMergeExec(planLater(child)) :: Nil
      case SmallFileMerge(child, noMerge) if noMerge =>
        SmallFileListExec(planLater(child)) :: Nil
      case _ => Nil
    }
  }
}
