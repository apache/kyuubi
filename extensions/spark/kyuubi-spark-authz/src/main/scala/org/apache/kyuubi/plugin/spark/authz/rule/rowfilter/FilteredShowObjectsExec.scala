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
package org.apache.kyuubi.plugin.spark.authz.rule.rowfilter

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec

import org.apache.kyuubi.plugin.spark.authz.{ObjectType, OperationType}
import org.apache.kyuubi.plugin.spark.authz.ranger.{AccessRequest, AccessResource, AccessType, SparkRangerAdminPlugin}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils

trait FilteredShowObjectsExec extends V2CommandExec with LeafExecNode {
  def result: Array[InternalRow]
}

trait FilteredShowObjectsCheck {
  def isAllowed(r: InternalRow, ugi: UserGroupInformation): Boolean
}

case class FilteredShowNamespaceExec(result: Array[InternalRow], output: Seq[Attribute])
  extends FilteredShowObjectsExec {
  override protected def run(): Seq[InternalRow] = result
}

object FilteredShowNamespaceExec extends FilteredShowObjectsCheck {
  def apply(delegated: SparkPlan, sc: SparkContext): FilteredShowNamespaceExec = {
    val result = delegated.executeCollect()
      .filter(isAllowed(_, AuthZUtils.getAuthzUgi(sc)))
    new FilteredShowNamespaceExec(result, delegated.output)
  }

  override def isAllowed(r: InternalRow, ugi: UserGroupInformation): Boolean = {
    val database = r.getString(0)
    val resource = AccessResource(ObjectType.DATABASE, database, null, null)
    val request = AccessRequest(resource, ugi, OperationType.SHOWDATABASES, AccessType.USE)
    val result = SparkRangerAdminPlugin.isAccessAllowed(request)
    result != null && result.getIsAllowed
  }
}

case class FilteredShowTablesExec(result: Array[InternalRow], output: Seq[Attribute])
  extends FilteredShowObjectsExec {
  override protected def run(): Seq[InternalRow] = result
}

object FilteredShowTablesExec extends FilteredShowObjectsCheck {
  def apply(delegated: SparkPlan, sc: SparkContext): FilteredShowNamespaceExec = {
    val result = delegated.executeCollect()
      .filter(isAllowed(_, AuthZUtils.getAuthzUgi(sc)))
    new FilteredShowNamespaceExec(result, delegated.output)
  }

  override def isAllowed(r: InternalRow, ugi: UserGroupInformation): Boolean = {
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
