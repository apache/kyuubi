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
import org.apache.spark.sql.catalyst.plans.logical.{DropTable, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.{AlterTableDropPartitionCommand, DropDatabaseCommand, DropFunctionCommand, DropTableCommand}

import org.apache.kyuubi.sql.KyuubiSQLConf._

case class DropIgnoreNonexistent(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.getConf(DROP_IGNORE_NONEXISTENT)) {
      plan match {
        case i @ AlterTableDropPartitionCommand(_, _, ifExists, _, _) if !ifExists =>
          i.copy(ifExists = true)
        case i @ DropTable(_, ifExists, _) if !ifExists =>
          i.copy(ifExists = true)
        case i @ DropTableCommand(_, ifExists, _, _) if !ifExists =>
          i.copy(ifExists = true)
        case i @ DropDatabaseCommand(_, ifExists, _) if !ifExists =>
          i.copy(ifExists = true)
        case i @ DropFunctionCommand(_, _, ifExists, _) if !ifExists =>
          i.copy(ifExists = true)
        case _ => plan
      }
    } else {
      plan
    }
  }

}
