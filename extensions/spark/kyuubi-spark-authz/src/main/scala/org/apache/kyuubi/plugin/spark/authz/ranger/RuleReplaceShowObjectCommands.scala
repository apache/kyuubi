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

package org.apache.kyuubi.plugin.spark.authz.ranger

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.RunnableCommand

import org.apache.kyuubi.plugin.spark.authz.{ObjectType, OperationType}
import org.apache.kyuubi.plugin.spark.authz.util.{AuthZUtils, ObjectFilterPlaceHolder, WithInternalChild}

class RuleReplaceShowObjectCommands extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case r: RunnableCommand if r.nodeName == "ShowTablesCommand" => FilteredShowTablesCommand(r)
    // show databases in spark2.4.x
    case r: RunnableCommand if r.nodeName == "ShowDatabasesCommand" =>
      FilteredShowDatabasesCommand(r)
    case n: LogicalPlan if n.nodeName == "ShowNamespaces" =>
      ObjectFilterPlaceHolder(n)
    case r: RunnableCommand if r.nodeName == "ShowFunctionsCommand" =>
      FilteredShowFunctionsCommand(r)
    case _ => plan
  }
}

case class FilteredShowTablesCommand(delegated: RunnableCommand)
  extends FilteredShowObjectCommand(delegated) {

  protected def isAllowed(r: Row, ugi: UserGroupInformation): Boolean = {
    val database = r.getString(0)
    val table = r.getString(1)
    val isTemp = r.getBoolean(2)
    val objectType = if (isTemp) ObjectType.VIEW else ObjectType.TABLE
    val resource = AccessResource(objectType, database, table, null)
    val request = AccessRequest(resource, ugi, OperationType.SHOWTABLES, AccessType.USE)
    val result = SparkRangerAdminPlugin.isAccessAllowed(request)
    result != null && result.getIsAllowed
  }
}

case class FilteredShowDatabasesCommand(delegated: RunnableCommand)
  extends FilteredShowObjectCommand(delegated) {

  override protected def isAllowed(r: Row, ugi: UserGroupInformation): Boolean = {
    val database = r.getString(0)
    val resource = AccessResource(ObjectType.DATABASE, database, null, null)
    val request = AccessRequest(resource, ugi, OperationType.SHOWDATABASES, AccessType.USE)
    val result = SparkRangerAdminPlugin.isAccessAllowed(request)
    result != null && result.getIsAllowed
  }
}

abstract class FilteredShowObjectCommand(delegated: RunnableCommand)
  extends RunnableCommand with WithInternalChild {

  override val output: Seq[Attribute] = delegated.output

  override def run(spark: SparkSession): Seq[Row] = {
    val rows = delegated.run(spark)
    val ugi = AuthZUtils.getAuthzUgi(spark.sparkContext)
    rows.filter(r => isAllowed(r, ugi))
  }

  protected def isAllowed(r: Row, ugi: UserGroupInformation): Boolean

  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this
}

case class FilteredShowFunctionsCommand(delegated: RunnableCommand)
  extends FilteredShowObjectCommand(delegated) with WithInternalChild {

  override protected def isAllowed(r: Row, ugi: UserGroupInformation): Boolean = {
    val functionName = r.getString(0)
    val items = functionName.split("\\.", 2)
    val resource =
      if (items.length == 1) {
        AccessResource(ObjectType.FUNCTION, null, items(0), null)
      } else {
        AccessResource(ObjectType.FUNCTION, items(0), items(1), null)
      }
    val request = AccessRequest(resource, ugi, OperationType.SHOWFUNCTIONS, AccessType.USE)
    val result = SparkRangerAdminPlugin.isAccessAllowed(request)
    result != null && result.getIsAllowed
  }
}
