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
import org.apache.kyuubi.plugin.spark.authz.util.{AuthZUtils, WithInternalChild}

class RuleReplaceShowObjectCommands extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case r: RunnableCommand if r.nodeName == "ShowTablesCommand" => FilteredShowTablesCommand(r)
    case r: ShowNamespaces if r.nodeName == "ShowNamespaces" =>
      FilteredShowDatabasesCommand(r)
    case _ => plan
  }
}

case class FilteredShowTablesCommand(delegated: RunnableCommand)
  extends RunnableCommand with WithInternalChild {

  override val output: Seq[Attribute] = delegated.output

  override def run(spark: SparkSession): Seq[Row] = {
    val rows = delegated.run(spark)
    val ugi = AuthZUtils.getAuthzUgi(spark.sparkContext)
    rows.filter(r => isAllowed(r, ugi))
  }

  private def isAllowed(r: Row, ugi: UserGroupInformation): Boolean = {
    val database = r.getString(0)
    val table = r.getString(1)
    val isTemp = r.getBoolean(2)
    val objectType = if (isTemp) ObjectType.VIEW else ObjectType.TABLE
    val resource = AccessResource(objectType, database, table, null)
    val request = AccessRequest(resource, ugi, OperationType.SHOWTABLES, AccessType.USE)
    val result = SparkRangerAdminPlugin.isAccessAllowed(request)
    result != null && result.getIsAllowed
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this
}

case class FilteredShowDatabasesCommand(delegated: ShowNamespaces) extends RunnableCommand
  with WithInternalChild {

  override val output: Seq[Attribute] = delegated.output

  override def run(spark: SparkSession): Seq[Row] = {
    val catalog = spark.sessionState.catalog
    val dbs = catalog.listDatabases()
    val rows = dbs.map(s => Row(s))
    val ugi = AuthZUtils.getAuthzUgi(spark.sparkContext)
    rows.filter(r => isAllowed(r, ugi))
  }

  protected def isAllowed(r: Row, ugi: UserGroupInformation): Boolean = {
    val database = r.getString(0)
    val objectType = ObjectType.DATABASE
    val resource = AccessResource(objectType, database, null, null)
    val request = AccessRequest(resource, ugi, OperationType.SHOWDATABASES, AccessType.USE)
    val result = SparkRangerAdminPlugin.isAccessAllowed(request)
    result != null && result.getIsAllowed
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this
}
