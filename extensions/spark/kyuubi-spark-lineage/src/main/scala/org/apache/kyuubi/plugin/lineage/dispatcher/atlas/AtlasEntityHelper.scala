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

import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId, AtlasRelatedObjectId}
import org.apache.spark.kyuubi.lineage.{LineageConf, SparkContextHelper}
import org.apache.spark.sql.execution.QueryExecution

import org.apache.kyuubi.plugin.lineage.Lineage
import org.apache.kyuubi.plugin.lineage.helper.SparkListenerHelper

/**
 * The helpers for Atlas spark entities from Lineage.
 * The Atlas spark models refer to :
 *    https://github.com/apache/atlas/blob/master/addons/models/1000-Hadoop/1100-spark_model.json
 */
object AtlasEntityHelper {

  /**
   * Generate `spark_process` Atlas Entity from Lineage.
   * @param qe
   * @param lineage
   * @return
   */
  def processEntity(qe: QueryExecution, lineage: Lineage): AtlasEntity = {
    val entity = new AtlasEntity(PROCESS_TYPE)

    val appId = SparkContextHelper.globalSparkContext.applicationId
    val appName = SparkContextHelper.globalSparkContext.appName match {
      case "Spark shell" => s"Spark Job $appId"
      case default => s"$default $appId"
    }

    entity.setAttribute("qualifiedName", appId)
    entity.setAttribute("name", appName)
    entity.setAttribute("currUser", SparkListenerHelper.currentUser)
    SparkListenerHelper.sessionUser.foreach(entity.setAttribute("remoteUser", _))
    entity.setAttribute("executionId", qe.id)
    entity.setAttribute("details", qe.toString())
    entity.setAttribute("sparkPlanDescription", qe.sparkPlan.toString())

    // TODO add entity type instead of parsing from string
    val inputs = lineage.inputTables.flatMap(tableObjectId).map { objId =>
      relatedObjectId(objId, RELATIONSHIP_DATASET_PROCESS_INPUTS)
    }
    val outputs = lineage.outputTables.flatMap(tableObjectId).map { objId =>
      relatedObjectId(objId, RELATIONSHIP_PROCESS_DATASET_OUTPUTS)
    }

    entity.setRelationshipAttribute("inputs", inputs.asJava)
    entity.setRelationshipAttribute("outputs", outputs.asJava)

    entity
  }

  /**
   * Generate `spark_column_lineage` Atlas Entity from Lineage.
   * @param processEntity
   * @param lineage
   * @return
   */
  def columnLineageEntities(processEntity: AtlasEntity, lineage: Lineage): Seq[AtlasEntity] = {
    lineage.columnLineage.flatMap(columnLineage => {
      val inputs = columnLineage.originalColumns.flatMap(columnObjectId).map { objId =>
        relatedObjectId(objId, RELATIONSHIP_DATASET_PROCESS_INPUTS)
      }
      val outputs = Option(columnLineage.column).flatMap(columnObjectId).map { objId =>
        relatedObjectId(objId, RELATIONSHIP_PROCESS_DATASET_OUTPUTS)
      }.toSeq

      if (inputs.nonEmpty && outputs.nonEmpty) {
        val entity = new AtlasEntity(COLUMN_LINEAGE_TYPE)
        val outputColumnName = buildColumnQualifiedName(columnLineage.column).get
        val qualifiedName =
          s"${processEntity.getAttribute("qualifiedName")}:${outputColumnName}"
        entity.setAttribute("qualifiedName", qualifiedName)
        entity.setAttribute("name", qualifiedName)
        entity.setRelationshipAttribute("inputs", inputs.asJava)
        entity.setRelationshipAttribute("outputs", outputs.asJava)
        entity.setRelationshipAttribute(
          "process",
          relatedObjectId(objectId(processEntity), RELATIONSHIP_SPARK_PROCESS_COLUMN_LINEAGE))
        Some(entity)
      } else {
        None
      }
    })
  }

  def tableObjectId(tableName: String): Option[AtlasObjectId] = {
    buildTableQualifiedName(tableName)
      .map(new AtlasObjectId(HIVE_TABLE_TYPE, "qualifiedName", _))
  }

  def buildTableQualifiedName(tableName: String): Option[String] = {
    val defaultCatalog = LineageConf.DEFAULT_CATALOG
    tableName.split('.') match {
      case Array(`defaultCatalog`, db, table) =>
        Some(s"${db.toLowerCase}.${table.toLowerCase}@$cluster")
      case _ =>
        None
    }
  }

  def columnObjectId(columnName: String): Option[AtlasObjectId] = {
    buildColumnQualifiedName(columnName)
      .map(new AtlasObjectId(HIVE_COLUMN_TYPE, "qualifiedName", _))
  }

  def buildColumnQualifiedName(columnName: String): Option[String] = {
    val defaultCatalog = LineageConf.DEFAULT_CATALOG
    columnName.split('.') match {
      case Array(`defaultCatalog`, db, table, column) =>
        Some(s"${db.toLowerCase}.${table.toLowerCase}.${column.toLowerCase}@$cluster")
      case _ =>
        None
    }
  }

  def objectId(entity: AtlasEntity): AtlasObjectId = {
    val objId = new AtlasObjectId(entity.getGuid, entity.getTypeName)
    objId.setUniqueAttributes(Map("qualifiedName" -> entity.getAttribute("qualifiedName")).asJava)
    objId
  }

  def relatedObjectId(objectId: AtlasObjectId, relationshipType: String): AtlasRelatedObjectId = {
    new AtlasRelatedObjectId(objectId, relationshipType)
  }

  lazy val cluster = AtlasClientConf.getConf().get(AtlasClientConf.CLUSTER_NAME)
  lazy val columnLineageEnabled =
    AtlasClientConf.getConf().get(AtlasClientConf.COLUMN_LINEAGE_ENABLED).toBoolean

  val HIVE_TABLE_TYPE = "hive_table"
  val HIVE_COLUMN_TYPE = "hive_column"
  val PROCESS_TYPE = "spark_process"
  val COLUMN_LINEAGE_TYPE = "spark_column_lineage"
  val RELATIONSHIP_DATASET_PROCESS_INPUTS = "dataset_process_inputs"
  val RELATIONSHIP_PROCESS_DATASET_OUTPUTS = "process_dataset_outputs"
  val RELATIONSHIP_SPARK_PROCESS_COLUMN_LINEAGE = "spark_process_column_lineages"

}
