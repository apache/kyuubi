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

import scala.language.implicitConversions

import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.plugin.spark.authz.{ObjectType, PrivilegeObject}
import org.apache.kyuubi.plugin.spark.authz.ObjectType._
import org.apache.kyuubi.plugin.spark.authz.OperationType.OperationType
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils

class AccessResource private (val objectType: ObjectType) extends RangerAccessResourceImpl {
  implicit def asString(obj: Object): String = if (obj != null) obj.asInstanceOf[String] else null
  def getDatabase: String = getValue("database")
  def getTable: String = getValue("table")
  def getColumn: String = getValue("column")
  def getColumns: Seq[String] = {
    val columnStr = getColumn
    if (columnStr == null) Nil else columnStr.split(",").filter(_.nonEmpty)
  }
}

object AccessResource {

  def apply(
      objectType: ObjectType,
      firstLevelResourceOrNull: String,
      secondLevelResource: String,
      thirdLevelResource: String,
      spark: SparkSession = null): AccessResource = {
    val resource = new AccessResource(objectType)

    def firstLevelResource: String = {
      if (spark != null
        && (firstLevelResourceOrNull == null || firstLevelResourceOrNull.isEmpty)) {
        if (AuthZUtils.isSparkVersionAtMost("2.4")) {
          spark.catalog.currentDatabase
        } else if (AuthZUtils.isSparkVersionAtLeast("3.0")) {
          val catalogManager = AuthZUtils.invoke(spark.sessionState, "catalogManager")
          val currentNamespace =
            AuthZUtils.invoke(catalogManager.asInstanceOf[AnyRef], "currentNamespace")
          val namespaces: Array[String] = currentNamespace.asInstanceOf[Array[String]]
          if (namespaces != null && namespaces.length == 1) {
            return namespaces.head
          }
        }
      }
      firstLevelResourceOrNull
    }

    resource.objectType match {
      case DATABASE => resource.setValue("database", firstLevelResource)
      case FUNCTION =>
        resource.setValue("database", Option(firstLevelResource).getOrElse(""))
        resource.setValue("udf", secondLevelResource)
      case COLUMN =>
        resource.setValue("database", firstLevelResource)
        resource.setValue("table", secondLevelResource)
        resource.setValue("column", thirdLevelResource)
      case TABLE | VIEW => // fixme spark have added index support
        resource.setValue("database", firstLevelResource)
        resource.setValue("table", secondLevelResource)
    }
    resource.setServiceDef(SparkRangerAdminPlugin.getServiceDef)
    resource
  }

  def apply(objectType: ObjectType, firstLevelResource: String): AccessResource = {
    apply(objectType, firstLevelResource, null, null, null)
  }

  def apply(
      objectType: ObjectType,
      firstLevelResource: String,
      spark: SparkSession): AccessResource = {
    apply(objectType, firstLevelResource, null, null, spark)
  }

  def apply(obj: PrivilegeObject, opType: OperationType): AccessResource = {
    apply(ObjectType(obj, opType), obj.dbname, obj.objectName, obj.columns.mkString(","), null)
  }

  def apply(obj: PrivilegeObject, opType: OperationType, spark: SparkSession): AccessResource = {
    apply(ObjectType(obj, opType), obj.dbname, obj.objectName, obj.columns.mkString(","), spark)
  }
}
