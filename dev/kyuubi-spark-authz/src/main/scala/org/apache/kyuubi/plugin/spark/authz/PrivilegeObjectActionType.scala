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

object PrivilegeObjectActionType extends Enumeration {
  type PrivilegeObjectActionType = Value

  val OTHER, INSERT, INSERT_OVERWRITE, UPDATE, DELETE = Value

  /**
   * Initializing PrivilegeObjectActionType from spark's SaveMode
   * @param saveMode string representation of org.apache.spark.sql.SaveMode
   * @return PrivilegeObjectActionType enum item
   */
  def apply(saveMode: String): PrivilegeObjectActionType = {
    saveMode.toLowerCase match {
      case "append" | "ignore" | "errorifexists" => INSERT
      case "overwrite" => INSERT_OVERWRITE
      case _ => OTHER
    }
  }
}
