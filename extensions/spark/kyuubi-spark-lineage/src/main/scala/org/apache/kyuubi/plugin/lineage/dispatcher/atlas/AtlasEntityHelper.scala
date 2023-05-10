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

package org.apache.kyuubi.plugin.lineage.dispatcher.atlas

import scala.collection.JavaConverters._

import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}
import org.apache.spark.kyuubi.lineage.SparkContextHelper
import org.apache.spark.sql.execution.QueryExecution

import org.apache.kyuubi.Utils
import org.apache.kyuubi.plugin.lineage.Lineage

object AtlasEntityHelper {

  def processEntity(qe: QueryExecution, lineage: Lineage): AtlasEntity = {
    val entity = new AtlasEntity(PROCESS_TYPE_STRING)

    val appId = SparkContextHelper.globalSparkContext.applicationId
    val appName = SparkContextHelper.globalSparkContext.appName match {
      case "Spark shell" => s"Spark Job + $appId"
      case default => default + s" $appId"
    }

    entity.setAttribute("qualifiedName", appId)
    entity.setAttribute("name", appName)
    entity.setAttribute("currUser", Utils.currentUser)
    entity.setAttribute("executionId", qe.id)
    entity.setAttribute("details", qe.toString())
    entity.setAttribute("sparkPlanDescription", qe.sparkPlan.toString())

    // TODO add entity type instead of parsing from string
    val inputs = lineage.inputTables.map(tableObjectId(_)).asJava
    val outputs = lineage.outputTables.map(tableObjectId(_)).asJava

    entity.setAttribute("inputs", inputs)
    entity.setAttribute("outputs", outputs)

    // TODO support spark_column_lineage

    entity
  }

  def tableObjectId(tableName: String): Option[AtlasObjectId] = {
    val dbTb = tableName.split('.')
    if (dbTb.length == 2) {
      val qualifiedName = tableQualifiedName(CLUSTER, dbTb(0), dbTb(1))
      // TODO parse datasource type
      Some(new AtlasObjectId(HIVE_TABLE_TYPE_STRING, "qualifiedName", qualifiedName))
    } else {
      None
    }
  }

  def tableQualifiedName(cluster: String, db: String, table: String): String = {
    s"${db.toLowerCase}.${table.toLowerCase}@$cluster"
  }

  val HIVE_TABLE_TYPE_STRING = "hive_table"
  val PROCESS_TYPE_STRING = "spark_process"
  lazy val CLUSTER = AtlasClientConf.getConf().get(AtlasClientConf.CLUSTER_NAME)

}
