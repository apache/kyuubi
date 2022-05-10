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
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolvedNamespace
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ShowNamespaces}
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.{ShowNamespacesExec, V2CommandExec}
import org.apache.spark.sql.execution.datasources.v2.SparkCatalogV2Bridge.SPARK_CATALOG_V2_IMPLICIT.CatalogHelper

import org.apache.kyuubi.plugin.spark.authz.{ObjectType, OperationType}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils

class FilterDataSourceV2Strategy extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ShowNamespaces(ResolvedNamespace(catalog, ns), pattern, output) =>
      FilterShowNamespaceExec(
        ShowNamespacesExec(output, catalog.asNamespaceCatalog, ns, pattern)) :: Nil

    case _ => Nil
  }
}

case class FilterShowNamespaceExec(delegated: V2CommandExec) extends ShowObjectExec(delegated) {

  override protected def isAllowed(r: InternalRow, ugi: UserGroupInformation): Boolean = {
    val database = r.getString(0)
    val resource = AccessResource(ObjectType.DATABASE, database, null, null)
    val request = AccessRequest(resource, ugi, OperationType.SHOWDATABASES, AccessType.USE)
    val result = SparkRangerAdminPlugin.isAccessAllowed(request)
    result != null && result.getIsAllowed
  }
}

abstract class ShowObjectExec(delegated: V2CommandExec)
  extends V2CommandExec with LeafExecNode {

  override def run(): Seq[InternalRow] = {
    val runMethod = delegated.getClass.getMethod("run")
    runMethod.setAccessible(true)
    val rows = runMethod.invoke(delegated).asInstanceOf[Seq[InternalRow]]
    val ugi = AuthZUtils.getAuthzUgi(sparkContext)
    rows.filter(r => isAllowed(r, ugi))
  }

  protected def isAllowed(r: InternalRow, ugi: UserGroupInformation): Boolean

  override def output: Seq[Attribute] = delegated.output

}
