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

import java.util.{Map => JMap, ServiceLoader}

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._

/**
 * A trait for extracting database and table as string tuple
 * from the give object whose class type is define by `key`.
 */
trait TableExtractor extends ((SparkSession, AnyRef) => Option[Table]) with Extractor

object TableExtractor {
  val tableExtractors: Map[String, TableExtractor] = {
    ServiceLoader.load(classOf[TableExtractor])
      .iterator()
      .asScala
      .map(e => (e.key, e))
      .toMap
  }

  /**
   * Get table owner from table properties
   * @param v a object contains a org.apache.spark.sql.connector.catalog.Table
   * @return owner
   */
  def getOwner(v: AnyRef): Option[String] = {
    // org.apache.spark.sql.connector.catalog.Table
    val table = invoke(v, "table")
    val properties = invokeAs[JMap[String, String]](table, "properties").asScala
    properties.get("owner")
  }
}

/**
 * TableIdentifier ->
 */
class TableIdentifierTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val identifier = v1.asInstanceOf[TableIdentifier]
    val owner =
      try {
        val catalogTable = spark.sessionState.catalog.getTableMetadata(identifier)
        Option(catalogTable.owner).filter(_.nonEmpty)
      } catch {
        case _: Exception => None
      }
    Some(Table(None, identifier.database, identifier.table, owner))
  }
}

/**
 * CatalogTable ->
 */
class CatalogTableTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val catalogTable = v1.asInstanceOf[CatalogTable]
    val identifier = catalogTable.identifier
    val owner = Option(catalogTable.owner).filter(_.nonEmpty)
    Some(Table(None, identifier.database, identifier.table, owner))
  }
}

/**
 * org.apache.spark.sql.catalyst.analysis.ResolvedTable
 */
class ResolvedTableTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val identifier = invoke(v1, "identifier")
    val maybeTable = new IdentifierTableExtractor().apply(spark, identifier)
    val maybeOwner = TableExtractor.getOwner(v1)
    maybeTable.map(_.copy(owner = maybeOwner))
  }
}

/**
 * org.apache.spark.sql.connector.catalog.Identifier
 */
class IdentifierTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val namespace = invokeAs[Array[String]](v1, "namespace")
    val table = invokeAs[String](v1, "name")
    Some(Table(None, Some(quote(namespace)), table, None))
  }
}

/**
 * DataSourceV2Relation ->
 */
class DataSourceV2RelationTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val plan = v1.asInstanceOf[LogicalPlan]
    val v2Relation = plan.find(_.getClass.getSimpleName == "DataSourceV2Relation")
    if (v2Relation.isEmpty) {
      None
    } else {
      val maybeIdentifier = invokeAs[Option[AnyRef]](v2Relation.get, "identifier")
      maybeIdentifier.flatMap { id =>
        val maybeTable = new IdentifierTableExtractor().apply(spark, id)
        val maybeOwner = TableExtractor.getOwner(v2Relation.get)
        maybeTable.map(_.copy(owner = maybeOwner))
      }
    }
  }
}

/**
 * org.apache.spark.sql.execution.datasources.LogicalRelation
 */
class LogicalRelationTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val maybeCatalogTable = invokeAs[Option[AnyRef]](v1, "catalogTable")
    maybeCatalogTable.flatMap { ct =>
      new CatalogTableTableExtractor().apply(spark, ct)
    }
  }
}

/**
 * ResolvedDbObjectName ->
 */
class ResolvedDbObjectNameTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val catalog = invoke(v1, "catalog")
    val catalogName = invokeAs[String](catalog, "name")
    val nameParts = invokeAs[Seq[String]](v1, "nameParts")
    val namespace = nameParts.init.toArray
    val table = nameParts.last
    Some(Table(Some(catalogName), Some(quote(namespace)), table, None))
  }
}
