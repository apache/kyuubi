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
import org.apache.kyuubi.plugin.spark.authz.serde.ActionTypeExtractor.actionTypeExtractors
import org.apache.kyuubi.plugin.spark.authz.serde.ColumnExtractor.columnExtractors
import org.apache.kyuubi.plugin.spark.authz.serde.DatabaseExtractor.dbExtractors
import org.apache.kyuubi.plugin.spark.authz.serde.FunctionExtractor.functionExtractors
import org.apache.kyuubi.plugin.spark.authz.serde.FunctionType.FunctionType
import org.apache.kyuubi.plugin.spark.authz.serde.FunctionTypeExtractor.functionTypeExtractors
import org.apache.kyuubi.plugin.spark.authz.serde.QueryExtractor.queryExtractors
import org.apache.kyuubi.plugin.spark.authz.serde.TableExtractor.tableExtractors
import org.apache.kyuubi.plugin.spark.authz.serde.TableType.TableType
import org.apache.kyuubi.plugin.spark.authz.serde.TableTypeExtractor.tableTypeExtractors
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._

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
   * The extractor/getter classname for the field
   */
  def fieldExtractor: String

  def getValue(v: AnyRef): AnyRef

  final def error(v: AnyRef, e: Throwable): String = {
    val resourceName = getClass.getSimpleName.stripSuffix("Desc")
    val objectClass = v.getClass.getName
    s"[Spark$SPARK_VERSION] failed to get $resourceName from $objectClass by" +
      s" $fieldExtractor/$fieldName, due to ${e.getMessage}"
  }
}

case class ColumnDesc(
    fieldName: String,
    fieldExtractor: String) extends Descriptor {

  override def getValue(v: AnyRef): Seq[String] = {
    val columnsVal = invoke(v, fieldName)
    val columnExtractor = columnExtractors(fieldExtractor)
    columnExtractor(columnsVal)
  }
}

case class DatabaseDesc(
    fieldName: String,
    fieldExtractor: String,
    isInput: Boolean = false) extends Descriptor {
  override def getValue(v: AnyRef): String = {
    val databaseVal = invoke(v, fieldName)
    val databaseExtractor = dbExtractors(fieldExtractor)
    databaseExtractor(databaseVal)
  }
}

case class FunctionTypeDesc(
    fieldName: String,
    fieldExtractor: String,
    skipTypes: Seq[String]) extends Descriptor {

  override def getValue(v: AnyRef): FunctionType = {
    getValue(v, SparkSession.active)
  }

  def getValue(v: AnyRef, spark: SparkSession): FunctionType = {
    val functionTypeVal = invoke(v, fieldName)
    val functionTypeExtractor = functionTypeExtractors(fieldExtractor)
    functionTypeExtractor(functionTypeVal, spark)
  }

  def skip(v: AnyRef, spark: SparkSession): Boolean = {
    skipTypes.exists(skipType => getValue(v, spark) == FunctionType.withName(skipType))
  }
}

case class FunctionDesc(
    fieldName: String,
    fieldExtractor: String,
    databaseDesc: Option[DatabaseDesc] = None,
    functionTypeDesc: Option[FunctionTypeDesc] = None,
    isInput: Boolean = false) extends Descriptor {
  override def getValue(v: AnyRef): Function = {
    val functionVal = invoke(v, fieldName)
    val functionExtractor = functionExtractors(fieldExtractor)
    var function = functionExtractor(functionVal)
    if (function.database.isEmpty) {
      function = function.copy(database = databaseDesc.map(_.getValue(v)))
    }
    function
  }
}

case class QueryDesc(
    fieldName: String,
    fieldExtractor: String = "LogicalPlanQueryExtractor") extends Descriptor {
  override def getValue(v: AnyRef): LogicalPlan = {
    val queryVal = invoke(v, fieldName)
    val queryExtractor = queryExtractors(fieldExtractor)
    queryExtractor(queryVal)
  }
}

case class TableTypeDesc(
    fieldName: String,
    fieldExtractor: String,
    skipTypes: Seq[String]) extends Descriptor {
  override def getValue(v: AnyRef): TableType = {
    getValue(v, SparkSession.active)
  }

  def getValue(v: AnyRef, spark: SparkSession): TableType = {
    val tableTypeVal = invoke(v, fieldName)
    val tableTypeExtractor = tableTypeExtractors(fieldExtractor)
    tableTypeExtractor(tableTypeVal, spark)
  }

  def skip(v: AnyRef): Boolean = {
    skipTypes.exists(skipType => getValue(v) == TableType.withName(skipType))
  }
}

case class TableDesc(
    fieldName: String,
    fieldExtractor: String,
    columnDesc: Option[ColumnDesc] = None,
    actionTypeDesc: Option[ActionTypeDesc] = None,
    tableTypeDesc: Option[TableTypeDesc] = None,
    isInput: Boolean = false,
    setCurrentDatabaseIfMissing: Boolean = false) extends Descriptor {
  override def getValue(v: AnyRef): Option[Table] = {
    getValue(v, SparkSession.active)
  }

  def getValue(v: AnyRef, spark: SparkSession): Option[Table] = {
    val tableVal = invoke(v, fieldName)
    val tableExtractor = tableExtractors(fieldExtractor)
    tableExtractor(spark, tableVal)
  }
}

case class ActionTypeDesc(
    fieldName: String,
    fieldExtractor: String,
    actionType: Option[String] = None) extends Descriptor {
  override def getValue(v: AnyRef): PrivilegeObjectActionType = {
    actionType.map(PrivilegeObjectActionType.withName).getOrElse {
      val actionTypeVal = invoke(v, fieldName)
      val extractor = actionTypeExtractors(fieldExtractor)
      extractor(actionTypeVal)
    }
  }
}
