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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import org.apache.kyuubi.plugin.spark.authz.util.PathIdentifier._
import org.apache.kyuubi.util.reflect.ReflectUtils.invokeAs

trait URIExtractor extends ((SparkSession, AnyRef) => Seq[Uri]) with Extractor

object URIExtractor {
  val uriExtractors: Map[String, URIExtractor] = {
    loadExtractorsToMap[URIExtractor]
  }
}

/**
 * String
 */
class StringURIExtractor extends URIExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Seq[Uri] = {
    v1 match {
      case uriPath: String => Seq(Uri(uriPath))
      case Some(uriPath: String) => Seq(Uri(uriPath))
      case _ => Nil
    }
  }
}

class StringSeqURIExtractor extends URIExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Seq[Uri] = {
    v1.asInstanceOf[Seq[String]].map(Uri)
  }
}

class CatalogStorageFormatURIExtractor extends URIExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Seq[Uri] = {
    v1.asInstanceOf[CatalogStorageFormat].locationUri.map(uri => Uri(uri.getPath)).toSeq
  }
}

class PropertiesPathUriExtractor extends URIExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Seq[Uri] = {
    v1.asInstanceOf[Map[String, String]].get("path").map(Uri).toSeq
  }
}

class PropertiesLocationUriExtractor extends URIExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Seq[Uri] = {
    v1.asInstanceOf[Map[String, String]].get("location").map(Uri).toSeq
  }
}

class BaseRelationFileIndexURIExtractor extends URIExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Seq[Uri] = {
    v1 match {
      case h: HadoopFsRelation => h.location.rootPaths.map(_.toString).map(Uri)
      case _ => Nil
    }
  }
}

class TableSpecURIExtractor extends URIExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Seq[Uri] = {
    new StringURIExtractor().apply(spark, invokeAs[Option[String]](v1, "location"))
  }
}

class CatalogTableURIExtractor extends URIExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Seq[Uri] = {
    v1.asInstanceOf[CatalogTable].storage.locationUri.map(_.toString).map(Uri).toSeq
  }
}

class PartitionLocsSeqURIExtractor extends URIExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Seq[Uri] = {
    v1.asInstanceOf[Seq[(_, Option[String])]].flatMap(_._2).map(Uri)
  }
}

class IdentifierURIExtractor extends URIExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Seq[Uri] = v1 match {
    case identifier: Identifier if isPathIdentifier(identifier.name(), spark) =>
      Seq(identifier.name()).map(Uri)
    case _ => Nil
  }
}

class SubqueryAliasURIExtractor extends URIExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Seq[Uri] = v1 match {
    case SubqueryAlias(_, SubqueryAlias(identifier, _)) =>
      if (isPathIdentifier(identifier.name, spark)) {
        Seq(identifier.name).map(Uri)
      } else {
        Nil
      }
    case SubqueryAlias(identifier, _) if isPathIdentifier(identifier.name, spark) =>
      Seq(identifier.name).map(Uri)
    case _ => Nil
  }
}

class DataSourceV2RelationURIExtractor extends URIExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Seq[Uri] = {
    val plan = v1.asInstanceOf[LogicalPlan]
    plan.find(_.getClass.getSimpleName == "DataSourceV2Relation").get match {
      case v2Relation: DataSourceV2Relation
          if v2Relation.identifier.isDefined &&
            isPathIdentifier(v2Relation.identifier.get.name, spark) =>
        Seq(v2Relation.identifier.get.name).map(Uri)
      case _ => Nil
    }
  }
}

class ResolvedTableURIExtractor extends URIExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Seq[Uri] = {
    val identifier = invokeAs[AnyRef](v1, "identifier")
    lookupExtractor[IdentifierURIExtractor].apply(spark, identifier)
  }
}

class TableIdentifierURIExtractor extends URIExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Seq[Uri] = v1 match {
    case tableIdentifier: TableIdentifier if isPathIdentifier(tableIdentifier.table, spark) =>
      Seq(tableIdentifier.table).map(Uri)
    case _ => Nil
  }
}

class TableIdentifierOptionURIExtractor extends URIExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Seq[Uri] = v1 match {
    case Some(tableIdentifier: TableIdentifier) =>
      lookupExtractor[TableIdentifierURIExtractor].apply(spark, tableIdentifier)
    case _ => Nil
  }
}
