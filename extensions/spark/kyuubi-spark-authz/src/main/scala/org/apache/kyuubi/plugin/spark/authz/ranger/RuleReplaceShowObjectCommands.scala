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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.ResolvedNamespace
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ShowNamespaces}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, StringUtils}
import org.apache.spark.sql.connector.catalog.{CatalogExtension, CatalogPlugin, SupportsNamespaces}
import org.apache.spark.sql.execution.command.{RunnableCommand, ShowColumnsCommand}

import org.apache.kyuubi.plugin.spark.authz.{ObjectType, OperationType}
import org.apache.kyuubi.plugin.spark.authz.ObjectType.{COLUMN, DATABASE}
import org.apache.kyuubi.plugin.spark.authz.util.{AuthZUtils, WithInternalChild}

class RuleReplaceShowObjectCommands extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case ShowNamespaces(ResolvedNamespace(catalog, ns), pattern, output) =>
      FilteredShowNamespacesCommand(output, catalog, ns, pattern)
    case r: RunnableCommand if r.nodeName == "ShowTablesCommand" => FilteredShowTablesCommand(r)
    case r: RunnableCommand if r.nodeName == "ShowColumnsCommand" => FilteredShowColumnsCommand(r)
    case _ => plan
  }
}

case class FilteredShowNamespacesCommand(
    outputAttribute: Seq[Attribute],
    catalog: CatalogPlugin,
    namespace: Seq[String],
    pattern: Option[String])
  extends RunnableCommand with WithInternalChild {

  override val output: Seq[Attribute] = outputAttribute

  override def run(spark: SparkSession): Seq[Row] = {
    val namespaces = catalog match {
      case catalog: CatalogExtension =>
        // DSv2 does not support pass schemaPattern transparently
        catalog.defaultNamespace() +: catalog.listNamespaces(Array())
      case catalog: SupportsNamespaces =>
        val rootSchema = catalog.listNamespaces()
        val allSchemas = listAllNamespaces(catalog, rootSchema)
        allSchemas
    }

    val ugi = AuthZUtils.getAuthzUgi(spark.sparkContext)
    val rows = new ArrayBuffer[Row]()
    namespaces.map(_.quoted).map { ns =>
      if (pattern.map(StringUtils.filterPattern(Seq(ns), _).nonEmpty).getOrElse(true)) {
        if (isAllowed(ns, ugi)) {
          rows += Row(ns)
        }
      }
    }

    rows.toSeq
  }

  private def listAllNamespaces(
      catalog: SupportsNamespaces,
      namespaces: Array[Array[String]]): Array[Array[String]] = {
    val children = namespaces.flatMap { ns =>
      catalog.listNamespaces(ns)
    }
    if (children.isEmpty) {
      namespaces
    } else {
      namespaces ++: listAllNamespaces(catalog, children)
    }
  }

  protected def isAllowed(database: String, ugi: UserGroupInformation): Boolean = {
    val resource = AccessResource(DATABASE, database)
    val request = AccessRequest(resource, ugi, OperationType.SHOWDATABASES, AccessType.USE)
    val result = SparkRangerAdminPlugin.isAccessAllowed(request)
    result != null && result.getIsAllowed
  }

  implicit class NamespaceHelper(namespace: Array[String]) {
    def quoted: String = namespace.map(quoteIfNeeded).mkString(".")
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this
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

case class FilteredShowColumnsCommand(delegated: RunnableCommand)
  extends RunnableCommand with WithInternalChild {

  override val output: Seq[Attribute] = delegated.output

  override def run(spark: SparkSession): Seq[Row] = {
    val rows = delegated.run(spark)
    val table = delegated.asInstanceOf[ShowColumnsCommand].tableName
    val ugi = AuthZUtils.getAuthzUgi(spark.sparkContext)
    rows.filter(f =>
      isAllowed(table.database.getOrElse("default"), table.table, f.getString(0), ugi))
  }

  protected def isAllowed(
      database: String,
      table: String,
      column: String,
      ugi: UserGroupInformation): Boolean = {
    val resource = AccessResource(COLUMN, database, table, column)
    val request = AccessRequest(resource, ugi, OperationType.SHOWCOLUMNS, AccessType.USE)
    val result = SparkRangerAdminPlugin.isAccessAllowed(request)
    result != null && result.getIsAllowed
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this
}
