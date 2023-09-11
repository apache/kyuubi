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

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.util.reflect.ReflectUtils._

/**
 * A trait for extracting database and table as string tuple
 * from the give object whose class type is define by `key`.
 */
trait TableExtractor extends ((SparkSession, AnyRef) => Option[Table]) with Extractor

object TableExtractor {
  val tableExtractors: Map[String, TableExtractor] = {
    loadExtractorsToMap[TableExtractor]
  }

  /**
   * Get table owner from table properties
   * @param v a object contains a org.apache.spark.sql.connector.catalog.Table
   * @return owner
   */
  def getOwner(v: AnyRef): Option[String] = {
    // org.apache.spark.sql.connector.catalog.Table
    val table = invokeAs[AnyRef](v, "table")
    val properties = invokeAs[JMap[String, String]](table, "properties").asScala
    properties.get("owner")
  }

  def getOwner(spark: SparkSession, catalogName: String, tableIdent: AnyRef): Option[String] = {
    try {
      val catalogManager = invokeAs[AnyRef](spark.sessionState, "catalogManager")
      val catalog = invokeAs[AnyRef](catalogManager, "catalog", (classOf[String], catalogName))
      val table = invokeAs[AnyRef](
        catalog,
        "loadTable",
        (Class.forName("org.apache.spark.sql.connector.catalog.Identifier"), tableIdent))
      getOwner(table)
    } catch {
      // Exception may occur due to invalid reflection or table not found
      case _: Exception => None
    }
  }
}

/**
 * org.apache.spark.sql.catalyst.TableIdentifier
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
 * org.apache.spark.sql.catalyst.catalog.CatalogTable
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
 * org.apache.spark.sql.catalyst.catalog.CatalogTable Option
 */
class CatalogTableOptionTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val catalogTable = v1.asInstanceOf[Option[CatalogTable]]
    catalogTable.flatMap(lookupExtractor[CatalogTableTableExtractor].apply(spark, _))
  }
}

/**
 * org.apache.spark.sql.catalyst.analysis.ResolvedTable
 */
class ResolvedTableTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val catalogVal = invokeAs[AnyRef](v1, "catalog")
    val catalog = lookupExtractor[CatalogPluginCatalogExtractor].apply(catalogVal)
    val identifier = invokeAs[AnyRef](v1, "identifier")
    val maybeTable = lookupExtractor[IdentifierTableExtractor].apply(spark, identifier)
    val maybeOwner = TableExtractor.getOwner(v1)
    maybeTable.map(_.copy(catalog = catalog, owner = maybeOwner))
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
 * java.lang.String
 * with concat parts by "."
 */
class StringTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val tableNameArr = v1.asInstanceOf[String].split("\\.")
    val maybeTable = tableNameArr.length match {
      case 1 => Table(None, None, tableNameArr(0), None)
      case 2 => Table(None, Some(tableNameArr(0)), tableNameArr(1), None)
      case 3 => Table(Some(tableNameArr(0)), Some(tableNameArr(1)), tableNameArr(2), None)
    }
    Option(maybeTable)
  }
}

/**
 * Seq[org.apache.spark.sql.catalyst.expressions.Expression]
 */
class ExpressionSeqTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val expressions = v1.asInstanceOf[Seq[Expression]]
    // Iceberg will rearrange the parameters according to the parameter order
    // defined in the procedure, where the table parameters are currently always the first.
    lookupExtractor[StringTableExtractor].apply(spark, expressions.head.toString())
  }
}

/**
 * org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
 */
class DataSourceV2RelationTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val plan = v1.asInstanceOf[LogicalPlan]
    val maybeV2Relation = plan.find(_.getClass.getSimpleName == "DataSourceV2Relation")
    maybeV2Relation match {
      case None => None
      case Some(v2Relation) =>
        val maybeCatalogPlugin = invokeAs[Option[AnyRef]](v2Relation, "catalog")
        val maybeCatalog = maybeCatalogPlugin.flatMap(catalogPlugin =>
          lookupExtractor[CatalogPluginCatalogExtractor].apply(catalogPlugin))
        lookupExtractor[TableTableExtractor].apply(spark, invokeAs[AnyRef](v2Relation, "table"))
          .map { table =>
            val maybeOwner = TableExtractor.getOwner(v2Relation)
            table.copy(catalog = maybeCatalog, owner = maybeOwner)
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
      lookupExtractor[CatalogTableTableExtractor].apply(spark, ct)
    }
  }
}

/**
 * org.apache.spark.sql.catalyst.analysis.ResolvedDbObjectName
 */
class ResolvedDbObjectNameTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val catalogVal = invokeAs[AnyRef](v1, "catalog")
    val catalog = lookupExtractor[CatalogPluginCatalogExtractor].apply(catalogVal)
    val nameParts = invokeAs[Seq[String]](v1, "nameParts")
    val namespace = nameParts.init.toArray
    val table = nameParts.last
    Some(Table(catalog, Some(quote(namespace)), table, None))
  }
}

/**
 * org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
 */
class ResolvedIdentifierTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    v1.getClass.getName match {
      case "org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier" =>
        val catalogVal = invokeAs[AnyRef](v1, "catalog")
        val catalog = lookupExtractor[CatalogPluginCatalogExtractor].apply(catalogVal)
        val identifier = invokeAs[AnyRef](v1, "identifier")
        val maybeTable = lookupExtractor[IdentifierTableExtractor].apply(spark, identifier)
        val owner = catalog.flatMap(name => TableExtractor.getOwner(spark, name, identifier))
        maybeTable.map(_.copy(catalog = catalog, owner = owner))
      case _ => None
    }
  }
}

/**
 * org.apache.spark.sql.connector.catalog.Table
 */
class TableTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val tableName = invokeAs[String](v1, "name")
    lookupExtractor[StringTableExtractor].apply(spark, tableName)
  }
}
