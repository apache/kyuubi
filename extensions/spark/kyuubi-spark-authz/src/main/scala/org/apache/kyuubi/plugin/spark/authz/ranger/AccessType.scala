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

import org.apache.kyuubi.plugin.spark.authz.{PrivilegeObject, PrivilegeObjectActionType}
import org.apache.kyuubi.plugin.spark.authz.OperationType._
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectType._

object AccessType extends Enumeration {

  type AccessType = Value

  val NONE, CREATE, ALTER, DROP, SELECT, UPDATE, USE, READ, WRITE, ALL, ADMIN = Value

  def apply(obj: PrivilegeObject, opType: OperationType, isInput: Boolean): AccessType = {
    obj.actionType match {
      case PrivilegeObjectActionType.OTHER => opType match {
          case CREATEDATABASE if obj.privilegeObjectType == DATABASE => CREATE
          case CREATEFUNCTION if obj.privilegeObjectType == FUNCTION => CREATE
          case CREATETABLE | CREATEVIEW | CREATETABLE_AS_SELECT
              if obj.privilegeObjectType == TABLE_OR_VIEW =>
            if (isInput) SELECT else CREATE
          case ALTERDATABASE |
              ALTERDATABASE_LOCATION |
              ALTERTABLE_ADDCOLS |
              ALTERTABLE_ADDPARTS |
              ALTERTABLE_DROPPARTS |
              ALTERTABLE_LOCATION |
              ALTERTABLE_RENAME |
              ALTERTABLE_PROPERTIES |
              ALTERTABLE_RENAMECOL |
              ALTERTABLE_RENAMEPART |
              ALTERTABLE_REPLACECOLS |
              ALTERTABLE_SERDEPROPERTIES |
              ALTERVIEW_RENAME |
              MSCK => ALTER
          case ALTERVIEW_AS => if (isInput) SELECT else ALTER
          case DROPDATABASE | DROPTABLE | DROPFUNCTION | DROPVIEW => DROP
          case LOAD => if (isInput) SELECT else UPDATE
          case QUERY |
              SHOW_CREATETABLE |
              SHOW_TBLPROPERTIES |
              SHOWPARTITIONS |
              ANALYZE_TABLE => SELECT
          case SHOWCOLUMNS | DESCTABLE => SELECT
          case SHOWDATABASES |
              SWITCHDATABASE |
              DESCDATABASE |
              SHOWTABLES |
              SHOWFUNCTIONS |
              DESCFUNCTION => USE
          case TRUNCATETABLE => UPDATE
          case _ => NONE
        }
      case PrivilegeObjectActionType.DELETE => DROP
      case _ => UPDATE
    }
  }
}
