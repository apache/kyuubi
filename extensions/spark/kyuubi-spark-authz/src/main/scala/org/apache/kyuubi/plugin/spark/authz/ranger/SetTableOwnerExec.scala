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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}

case class SetTableOwnerExec(
    delegated: SparkPlan,
    tableId: TableIdentifier,
    authzUser: String,
    replace: Boolean)
  extends LeafExecNode {

  final override def output: Seq[Attribute] = delegated.output

  final override def doExecute(): RDD[InternalRow] = {
    // `SparkPlan#session` not exists in Spark 3.0
    val catalog = SparkSession.active.sessionState.catalog
    val shouldSet = replace || !catalog.tableExists(tableId)

    val ret = delegated.executeCollect()

    if (shouldSet) {
      val metadata = catalog.getTableMetadata(tableId)
      if (metadata.owner != authzUser) {
        catalog.alterTable(metadata.copy(owner = authzUser))
      }
    }
    sparkContext.parallelize(ret, 1)
  }
}
