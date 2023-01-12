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

package org.apache.kyuubi.plugin.spark.authz.serde

import java.util
import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, StagedTable, StagingTableCatalog, Table => V2Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.plugin.spark.authz.ranger.{FilteredShowNamespaceExec, FilteredShowTablesExec}
import org.apache.kyuubi.plugin.spark.authz.util.{DelegatedStagingTableCatalog, DelegatedTableCatalog}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.{getAuthzUgi, getFieldVal, setFieldVal}

trait SparkPlanRewriter extends Rewriter {

  def rewrite(spark: SparkSession, plan: SparkPlan): SparkPlan

}

object SparkPlanRewriter {
  val sparkPlanRewriters: Map[String, SparkPlanRewriter] = {
    ServiceLoader.load(classOf[SparkPlanRewriter])
      .iterator()
      .asScala
      .map(e => (e.key, e))
      .toMap
  }
}

class ShowNamespaceExecRewriter extends SparkPlanRewriter {
  override def rewrite(spark: SparkSession, plan: SparkPlan): SparkPlan = {
    FilteredShowNamespaceExec(plan)
  }
}

class ShowTablesExecRewriter extends SparkPlanRewriter {
  override def rewrite(spark: SparkSession, plan: SparkPlan): SparkPlan = {
    FilteredShowTablesExec(plan)
  }
}

class CreateTableV2ExecRewriter extends SparkPlanRewriter {

  override def rewrite(spark: SparkSession, plan: SparkPlan): SparkPlan = {
    def rewriteOwner(properties: util.Map[String, String]): util.Map[String, String] = {
      val props =
        properties.asScala ++ Map("owner" -> getAuthzUgi(spark.sparkContext).getShortUserName)
      props.asJava
    }

    val delegated = getFieldVal[TableCatalog](plan, "catalog") match {
      case catalog: StagingTableCatalog =>
        new DelegatedStagingTableCatalog(catalog) {
          override def createTable(
              ident: Identifier,
              schema: StructType,
              partitions: Array[Transform],
              properties: util.Map[String, String]): V2Table = {
            super.createTable(ident, schema, partitions, rewriteOwner(properties))
          }

          override def stageCreate(
              ident: Identifier,
              schema: StructType,
              partitions: Array[Transform],
              properties: util.Map[String, String]): StagedTable = {
            super.stageCreate(ident, schema, partitions, rewriteOwner(properties))
          }

          override def stageReplace(
              ident: Identifier,
              schema: StructType,
              partitions: Array[Transform],
              properties: util.Map[String, String]): StagedTable = {
            super.stageReplace(ident, schema, partitions, rewriteOwner(properties))
          }

          override def stageCreateOrReplace(
              ident: Identifier,
              schema: StructType,
              partitions: Array[Transform],
              properties: util.Map[String, String]): StagedTable = {
            super.stageCreateOrReplace(ident, schema, partitions, rewriteOwner(properties))
          }
        }

      case catalog: TableCatalog =>
        new DelegatedTableCatalog(catalog) {
          override def createTable(
              ident: Identifier,
              schema: StructType,
              partitions: Array[Transform],
              properties: util.Map[String, String]): V2Table = {
            super.createTable(ident, schema, partitions, rewriteOwner(properties))
          }
        }
    }
    setFieldVal(plan, "catalog", delegated)
    plan
  }
}
