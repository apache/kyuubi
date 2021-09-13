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

package org.apache.kyuubi.sql.watchdog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{GlobalLimit, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.execution.command.InsertIntoDataSourceDirCommand

import org.apache.kyuubi.sql.KyuubiSQLConf

/*
* Add ForcedMaxOutputRows rule for output rows limitation
* to avoid huge output rows of non_limit query unexpectedly
* mainly applied to cases as below:
* case 1:
*  {{{
*   INSERT OVERWRITE DIRECTORY (path=STRING)?
*   USING format OPTIONS ([option1_name "option1_value", option2_name "option2_value", ...])
*   SELECT ...
* }}}
*
* case 2:
* {{{
*   SELECT [c1, c2, ...]
* }}}
*
* case 3:
* {{{
*   WITH CTE AS (
*   ...)
* SELECT [c1, c2, ...] FROM CTE ...
* }}}
*
* The Logical Rule add a GlobalLimit node before root project
* */
case class ForcedMaxOutputRowsRule(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.resolved) {
      plan
    } else {
      conf.getConf(KyuubiSQLConf.WATCHDOG_FORCED_MAXOUTPUTROWS) match {
        case Some(forcedMaxOutputRows) => plan match {
          case insert@InsertIntoDataSourceDirCommand(_, _, Project(_, _), _) =>
            insert.copy(query = GlobalLimit(forcedMaxOutputRows, insert.query))
          case project@Project(_, _) => project.origin match {
            case Origin(None, None) => GlobalLimit(forcedMaxOutputRows, project)
            case Origin(Some(_), Some(0)) => GlobalLimit(forcedMaxOutputRows, project)
            case _ => project
          }
          case _ => plan
        }
        case _ => plan
      }
    }
  }
}
