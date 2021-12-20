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
import org.apache.spark.sql.catalyst.analysis.{UnresolvedRelation, UnresolvedTableOrView, UnresolvedView}
import org.apache.spark.sql.catalyst.plans.logical.{DropTable, DropView, LogicalPlan, NoopCommand, UncacheTable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.{AlterTableDropPartitionCommand, DropDatabaseCommand, DropFunctionCommand, DropTableCommand}

import org.apache.kyuubi.sql.KyuubiSQLConf._

case class DropIgnoreNonexistent(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.getConf(DROP_IGNORE_NONEXISTENT)) {
      plan match {
        case i @ AlterTableDropPartitionCommand(_, _, false, _, _) =>
          i.copy(ifExists = true)
        case i @ DropTableCommand(_, false, _, _) =>
          i.copy(ifExists = true)
        case i @ DropDatabaseCommand(_, false, _) =>
          i.copy(ifExists = true)
        case i @ DropFunctionCommand(_, _, false, _) =>
          i.copy(ifExists = true)
        // like: org.apache.spark.sql.catalyst.analysis.ResolveCommandsWithIfExists
        case DropTable(u: UnresolvedTableOrView, false, _) =>
          NoopCommand("DROP TABLE", u.multipartIdentifier)
        case DropView(u: UnresolvedView, false) =>
          NoopCommand("DROP VIEW", u.multipartIdentifier)
        case UncacheTable(u: UnresolvedRelation, false, _) =>
          NoopCommand("UNCACHE TABLE", u.multipartIdentifier)
        case _ => plan
      }
    } else {
      plan
    }
  }

}
