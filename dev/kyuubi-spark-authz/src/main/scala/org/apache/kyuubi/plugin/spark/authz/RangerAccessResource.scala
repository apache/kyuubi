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

package org.apache.kyuubi.plugin.spark.authz

import scala.language.implicitConversions

import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl

import org.apache.kyuubi.plugin.spark.authz.ObjectType._
import org.apache.kyuubi.plugin.spark.authz.OperationType.OperationType

class RangerAccessResource private (val objectType: ObjectType) extends RangerAccessResourceImpl {
  implicit def asString(obj: Object): String = if (obj != null) obj.asInstanceOf[String] else null
  def getDatabase: String = getValue("database")
  def getTable: String = getValue("table")
  def getColumn: String = getValue("column")
  def getColumns: Array[String] = {
    val columnStr = getColumn
    columnStr.split(",").filter(_.isEmpty)
  }
}

object RangerAccessResource {

  def apply(
      objectType: ObjectType,
      firstLevelResource: String,
      secondLevelResource: String,
      thirdLevelResource: String): RangerAccessResource = {
    val resource = new RangerAccessResource(objectType)

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
      case _ =>
    }
    resource.setServiceDef(RangerSparkPlugin.getServiceDef)
    resource
  }

  def apply(
      objectType: ObjectType,
      firstLevelResource: String,
      secondLevelResource: String): RangerAccessResource = {
    apply(objectType, firstLevelResource, secondLevelResource, null)
  }

  def apply(
      objectType: ObjectType,
      firstLevelResource: String): RangerAccessResource = {
    apply(objectType, firstLevelResource, null)
  }

  def apply(obj: PrivilegeObject): RangerAccessResource = {
    obj.typ match {
      case PrivilegeObjectType.DATABASE => apply(DATABASE, obj.dbname)
      case PrivilegeObjectType.TABLE_OR_VIEW =>
        apply(TABLE, obj.dbname, obj.objectName)
      case PrivilegeObjectType.COLUMN if obj.columns != null =>
        apply(COLUMN, obj.dbname, obj.objectName, obj.columns.mkString(","))
      case _ => null
    }
  }
  def apply(
      obj: PrivilegeObject,
      opType: OperationType): RangerAccessResource = {
    apply(ObjectType(obj, opType), obj.dbname, obj.objectName, obj.columns.mkString(","))
  }
}
