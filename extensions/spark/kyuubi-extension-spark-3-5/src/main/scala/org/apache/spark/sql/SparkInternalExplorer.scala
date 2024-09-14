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

import org.apache.spark.internal.config.{ConfigEntry, SPECULATION_ENABLED}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.LocalTempView
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.execution.datasources.v2.BaseCacheTableExec
import org.apache.spark.util.ThreadUtils

object SparkInternalExplorer {

  val SPECULATION_ENABLED_SYNONYM: ConfigEntry[Boolean] = SPECULATION_ENABLED
  def parmap[I, O](in: Seq[I], prefix: String, maxThreads: Int)(f: I => O): Seq[O] =
    ThreadUtils.parmap(in, prefix, maxThreads)(f)

  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame =
    Dataset.ofRows(sparkSession, logicalPlan)

  case class CacheTableAsSelectExec(tempViewName: String, query: LogicalPlan)
    extends BaseCacheTableExec {
    override lazy val relationName: String = tempViewName
    override lazy val planToCache: LogicalPlan = {
      CreateViewCommand(
        name = TableIdentifier(tempViewName),
        userSpecifiedColumns = Nil,
        comment = None,
        properties = Map.empty,
        originalText = None,
        plan = query,
        allowExisting = true,
        replace = true,
        viewType = LocalTempView,
        isAnalyzed = true,
        referredTempFunctions = Seq.empty).run(session)

      dataFrameForCachedPlan.logicalPlan
    }
    override lazy val dataFrameForCachedPlan: DataFrame = {
      session.table(tempViewName)
    }

    override def isLazy: Boolean = false

    override def options: Map[String, String] = Map.empty
  }

}
