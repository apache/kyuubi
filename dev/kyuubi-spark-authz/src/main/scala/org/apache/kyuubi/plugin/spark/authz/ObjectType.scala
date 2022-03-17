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

import org.apache.kyuubi.plugin.spark.authz.OperationType.OperationType

object ObjectType extends Enumeration {

  type ObjectType = Value

  val
  NONE,
  DATABASE,
  TABLE,
  VIEW,
  COLUMN,
  FUNCTION = Value

  def apply(obj: PrivilegeObject, opType: OperationType): ObjectType = {
    obj.typ match {
      case PrivilegeObjectType.DATABASE | null => DATABASE
      case PrivilegeObjectType.TABLE_OR_VIEW
          if obj.columns != null && obj.columns.nonEmpty => COLUMN
      case PrivilegeObjectType.TABLE_OR_VIEW
          if StringUtils.containsIgnoreCase(opType.toString, "view") => VIEW
      case PrivilegeObjectType.TABLE_OR_VIEW => TABLE
      case PrivilegeObjectType.FUNCTION => FUNCTION
      case _ => NONE
    }
  }
}
