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

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectActionType.PrivilegeObjectActionType
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectType.PrivilegeObjectType

case class PrivilegeObject(
    privilegeObjectType: PrivilegeObjectType,
    actionType: PrivilegeObjectActionType,
    dbname: String,
    objectName: String,
    columns: Seq[String]) extends Comparable[PrivilegeObject] {

  private def compare(left: Seq[String], right: Seq[String]): Int = {
    if (left == right) {
      0
    } else if (left == null) {
      -1
    } else if (right == null) {
      1
    } else {
      left.zip(right).foreach { case (l, r) =>
        val comp = StringUtils.compare(l, r)
        if (comp != 0) {
          return comp
        }
      }
      left.size - right.size
    }
  }

  override def compareTo(o: PrivilegeObject): Int = {
    var comp = privilegeObjectType.compareTo(o.privilegeObjectType)
    if (comp == 0) {
      comp = StringUtils.compare(dbname, o.dbname)
      if (comp == 0) {
        comp = StringUtils.compare(objectName, o.objectName)
        if (comp == 0) {
          compare(columns, o.columns)
        } else {
          comp
        }
      } else {
        comp
      }
    } else {
      comp
    }
  }
}

object PrivilegeObject {
  def apply(
      typ: PrivilegeObjectType,
      actionType: PrivilegeObjectActionType,
      dbname: String,
      objectName: String,
      columns: Seq[String]): PrivilegeObject = {
    new PrivilegeObject(typ, actionType, dbname, objectName, columns)
  }

  def apply(
      typ: PrivilegeObjectType,
      actionType: PrivilegeObjectActionType,
      dbname: String,
      objectName: String): PrivilegeObject = {
    new PrivilegeObject(typ, actionType, dbname, objectName, Nil)
  }
}
