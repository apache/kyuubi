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

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectActionType
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectActionType.PrivilegeObjectActionType
import org.apache.kyuubi.plugin.spark.authz.serde.FunctionType.FunctionType
import org.apache.kyuubi.plugin.spark.authz.serde.TableType.TableType
import org.apache.kyuubi.util.reflect.ReflectUtils._

/**
 * A database object(such as database, table, function) descriptor describes its name and getter
 * in/from another object(such as a spark sql command).
 */
sealed trait Descriptor {

  /**
   * Describes the field name, such as a table field name in the spark logical plan.
   *
   * @return the database object field name
   */
  def fieldName: String

  /**
   * The key that points the [[Extractor]] to for the field, which extracts key information
   * we want to represents a table, column list, etc.
   */
  def fieldExtractor: String

  /**
   * Apply the field [[Extractor]] to the field value to extract key information we want to
   * represents a table, column list, etc.
   * @param v
   * @return
   */
  def extract(v: AnyRef): AnyRef

  def comment: String

  final def error(v: AnyRef, e: Throwable): String = {
    val resourceName = getClass.getSimpleName.stripSuffix("Desc")
    val objectClass = v.getClass.getName
    s"[Spark$SPARK_VERSION] failed to get $resourceName from $objectClass by" +
      s" $fieldExtractor/$fieldName, " +
      (if (comment.nonEmpty) s"desc comment: ${comment}") +
      s"due to ${e.getMessage}"
  }
}

/**
 * Column Descriptor
 *
 * @param fieldName the field name or method name of this column field
 * @param fieldExtractor the key of a [[ColumnExtractor]] instance
 */
case class ColumnDesc(
    fieldName: String,
    fieldExtractor: String,
    comment: String = "") extends Descriptor {
  override def extract(v: AnyRef): Seq[String] = {
    val columnsVal = invokeAs[AnyRef](v, fieldName)
    val columnExtractor = lookupExtractor[ColumnExtractor](fieldExtractor)
    columnExtractor(columnsVal)
  }
}

/**
 * Database Descriptor
 *
 * @param fieldName the field name or method name of this database field
 * @param fieldExtractor the key of a [[DatabaseExtractor]] instance
 * @param isInput read or write
 */
case class DatabaseDesc(
    fieldName: String,
    fieldExtractor: String,
    catalogDesc: Option[CatalogDesc] = None,
    isInput: Boolean = false,
    comment: String = "") extends Descriptor {
  override def extract(v: AnyRef): Database = {
    val databaseVal = invokeAs[AnyRef](v, fieldName)
    val databaseExtractor = lookupExtractor[DatabaseExtractor](fieldExtractor)
    val db = databaseExtractor(databaseVal)
    if (db.catalog.isEmpty && catalogDesc.nonEmpty) {
      val maybeCatalog = catalogDesc.get.extract(v)
      db.copy(catalog = maybeCatalog)
    } else {
      db
    }
  }
}

/**
 * Function Type Descriptor
 *
 * @param fieldName the field name or method name of this function type field
 * @param fieldExtractor the key of a [[FunctionTypeExtractor]] instance
 * @param skipTypes which kinds of functions are skipped checking
 */
case class FunctionTypeDesc(
    fieldName: String,
    fieldExtractor: String,
    skipTypes: Seq[String],
    comment: String = "") extends Descriptor {
  override def extract(v: AnyRef): FunctionType = {
    extract(v, SparkSession.active)
  }

  def extract(v: AnyRef, spark: SparkSession): FunctionType = {
    val functionTypeVal = invokeAs[AnyRef](v, fieldName)
    val functionTypeExtractor = lookupExtractor[FunctionTypeExtractor](fieldExtractor)
    functionTypeExtractor(functionTypeVal, spark)
  }

  def skip(v: AnyRef, spark: SparkSession): Boolean = {
    skipTypes.exists(skipType => extract(v, spark) == FunctionType.withName(skipType))
  }
}

/**
 * Function Descriptor
 *
 * @param fieldName the field name or method name of this function field
 * @param fieldExtractor the key of a [[FunctionExtractor]] instance
 * @param databaseDesc which kinds of functions are skipped checking
 * @param functionTypeDesc indicates the function type if necessary
 * @param isInput read or write
 */
case class FunctionDesc(
    fieldName: String,
    fieldExtractor: String,
    databaseDesc: Option[DatabaseDesc] = None,
    functionTypeDesc: Option[FunctionTypeDesc] = None,
    isInput: Boolean = false,
    comment: String = "") extends Descriptor {
  override def extract(v: AnyRef): Function = {
    val functionVal = invokeAs[AnyRef](v, fieldName)
    val functionExtractor = lookupExtractor[FunctionExtractor](fieldExtractor)
    var function = functionExtractor(functionVal)
    if (function.database.isEmpty) {
      val maybeDatabase = databaseDesc.map(_.extract(v))
      if (maybeDatabase.isDefined) {
        function = function.copy(database = Some(maybeDatabase.get.database))
      }
    }
    function
  }
}

/**
 * Query Descriptor which represents one or more query fields of a command
 *
 * @param fieldName the field name or method name of this query field
 * @param fieldExtractor the key of a [[QueryExtractor]] instance
 *                       The default value is [[LogicalPlanQueryExtractor]] which
 *                       return the original plan directly.
 */
case class QueryDesc(
    fieldName: String,
    fieldExtractor: String = "LogicalPlanQueryExtractor",
    comment: String = "") extends Descriptor {
  override def extract(v: AnyRef): Option[LogicalPlan] = {
    val queryVal = invokeAs[AnyRef](v, fieldName)
    val queryExtractor = lookupExtractor[QueryExtractor](fieldExtractor)
    queryExtractor(queryVal)
  }
}

/**
 * Table Type Descriptor
 *
 * @param fieldName the field name or method name of this table type field
 * @param fieldExtractor the key of a [[TableTypeExtractor]] instance
 * @param skipTypes which kinds of table or view are skipped checking
 */
case class TableTypeDesc(
    fieldName: String,
    fieldExtractor: String,
    skipTypes: Seq[String],
    comment: String = "") extends Descriptor {
  override def extract(v: AnyRef): TableType = {
    extract(v, SparkSession.active)
  }

  def extract(v: AnyRef, spark: SparkSession): TableType = {
    val tableTypeVal = invokeAs[AnyRef](v, fieldName)
    val tableTypeExtractor = lookupExtractor[TableTypeExtractor](fieldExtractor)
    tableTypeExtractor(tableTypeVal, spark)
  }

  def skip(v: AnyRef): Boolean = {
    skipTypes.exists(skipType => extract(v) == TableType.withName(skipType))
  }
}

/**
 * Table Descriptor
 *
 * @param fieldName the field name or method name of this table field
 * @param fieldExtractor the key of a [[TableExtractor]] instance
 * @param columnDesc optional [[ColumnDesc]] instance if columns field are specified
 * @param actionTypeDesc optional [[ActionTypeDesc]] indicates the action type
 * @param tableTypeDesc optional [[TableTypeDesc]] indicates the table type
 * @param catalogDesc optional [[CatalogDesc]] instance if a catalog field is specified,
 *                    the catalog will respect the one resolved from `fieldExtractor` first
 * @param isInput read or write
 * @param setCurrentDatabaseIfMissing whether to use current database if the database
 *                                    field is missing
 */
case class TableDesc(
    fieldName: String,
    fieldExtractor: String,
    columnDesc: Option[ColumnDesc] = None,
    actionTypeDesc: Option[ActionTypeDesc] = None,
    tableTypeDesc: Option[TableTypeDesc] = None,
    catalogDesc: Option[CatalogDesc] = None,
    isInput: Boolean = false,
    setCurrentDatabaseIfMissing: Boolean = false,
    comment: String = "") extends Descriptor {
  override def extract(v: AnyRef): Option[Table] = {
    extract(v, SparkSession.active)
  }

  def extract(v: AnyRef, spark: SparkSession): Option[Table] = {
    val tableVal = invokeAs[AnyRef](v, fieldName)
    val tableExtractor = lookupExtractor[TableExtractor](fieldExtractor)
    val maybeTable = tableExtractor(spark, tableVal)
    maybeTable.map { t =>
      if (t.catalog.isEmpty && catalogDesc.nonEmpty) {
        val newCatalog = catalogDesc.get.extract(v)
        t.copy(catalog = newCatalog)
      } else {
        t
      }
    }
  }
}

/**
 * Action Type Descriptor
 *
 * @param fieldName the field name or method name of this action type field
 * @param fieldExtractor the key of a [[ActionTypeExtractor]] instance
 * @param actionType the explicitly given action type which take precedence over extracting
 */
case class ActionTypeDesc(
    fieldName: String = null,
    fieldExtractor: String = null,
    actionType: Option[String] = None,
    comment: String = "") extends Descriptor {
  override def extract(v: AnyRef): PrivilegeObjectActionType = {
    actionType.map(PrivilegeObjectActionType.withName).getOrElse {
      val actionTypeVal = invokeAs[AnyRef](v, fieldName)
      val actionTypeExtractor = lookupExtractor[ActionTypeExtractor](fieldExtractor)
      actionTypeExtractor(actionTypeVal)
    }
  }
}

/**
 * Catalog Descriptor
 *
 * @param fieldName the field name or method name of this catalog field
 * @param fieldExtractor the key of a [[CatalogExtractor]] instance
 */
case class CatalogDesc(
    fieldName: String = "catalog",
    fieldExtractor: String = "CatalogPluginCatalogExtractor",
    comment: String = "") extends Descriptor {
  override def extract(v: AnyRef): Option[String] = {
    val catalogVal = invokeAs[AnyRef](v, fieldName)
    val catalogExtractor = lookupExtractor[CatalogExtractor](fieldExtractor)
    catalogExtractor(catalogVal)
  }
}

case class ScanDesc(
    fieldName: String,
    fieldExtractor: String,
    catalogDesc: Option[CatalogDesc] = None,
    comment: String = "") extends Descriptor {
  override def extract(v: AnyRef): Option[Table] = {
    extract(v, SparkSession.active)
  }

  def extract(v: AnyRef, spark: SparkSession): Option[Table] = {
    val tableVal = if (fieldName == null) {
      v
    } else {
      invokeAs[AnyRef](v, fieldName)
    }
    val tableExtractor = lookupExtractor[TableExtractor](fieldExtractor)
    val maybeTable = tableExtractor(spark, tableVal)
    maybeTable.map { t =>
      if (t.catalog.isEmpty && catalogDesc.nonEmpty) {
        val newCatalog = catalogDesc.get.extract(v)
        t.copy(catalog = newCatalog)
      } else {
        t
      }
    }
  }
}

/**
 * URI Descriptor
 *
 * @param fieldName the field name or method name of this uri field
 * @param fieldExtractor the key of a [[URIExtractor]] instance
 * @param isInput read or write
 */
case class UriDesc(
    fieldName: String,
    fieldExtractor: String,
    isInput: Boolean = false,
    comment: String = "") extends Descriptor {
  override def extract(v: AnyRef): Seq[Uri] = {
    extract(v, SparkSession.active)
  }

  def extract(v: AnyRef, spark: SparkSession): Seq[Uri] = {
    val uriVal = invokeAs[AnyRef](v, fieldName)
    val uriExtractor = lookupExtractor[URIExtractor](fieldExtractor)
    uriExtractor(spark, uriVal)
  }
}
