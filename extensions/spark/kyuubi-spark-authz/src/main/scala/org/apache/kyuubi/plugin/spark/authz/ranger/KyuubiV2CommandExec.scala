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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}

import org.apache.kyuubi.plugin.spark.authz.{ObjectType, OperationType}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils

abstract class KyuubiV2CommandExec extends SparkPlan {

  /**
   * Abstract method that each concrete command needs to implement to compute the result.
   */
  protected def run(): Seq[InternalRow]

  /**
   * The value of this field can be used as the contents of the corresponding RDD generated from
   * the physical plan of this command.
   */
  private lazy val result: Seq[InternalRow] = run()

  /**
   * The `execute()` method of all the physical command classes should reference `result`
   * so that the command can be executed eagerly right after the command query is created.
   */
  override def executeCollect(): Array[InternalRow] = result.toArray

  override def executeToIterator(): Iterator[InternalRow] = result.toIterator

  override def executeTake(limit: Int): Array[InternalRow] = result.take(limit).toArray

  override protected def doExecute(): RDD[InternalRow] = {
    sparkContext.parallelize(result, 1)
  }

  override def producedAttributes: AttributeSet = outputSet

}

abstract class ShowObjectExec(delegated: SparkPlan)
  extends KyuubiV2CommandExec with LeafExecNode {

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

case class FilterShowNamespaceExec(delegated: SparkPlan) extends ShowObjectExec(delegated) {

  override protected def isAllowed(r: InternalRow, ugi: UserGroupInformation): Boolean = {
    val database = r.getString(0)
    val resource = AccessResource(ObjectType.DATABASE, database, null, null)
    val request = AccessRequest(resource, ugi, OperationType.SHOWDATABASES, AccessType.USE)
    val result = SparkRangerAdminPlugin.isAccessAllowed(request)
    result != null && result.getIsAllowed
  }
}
