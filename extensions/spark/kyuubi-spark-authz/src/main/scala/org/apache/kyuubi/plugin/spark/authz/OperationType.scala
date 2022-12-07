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

object OperationType extends Enumeration {

  type OperationType = Value

  val ALTERDATABASE, ALTERDATABASE_LOCATION, ALTERTABLE_ADDCOLS, ALTERTABLE_ADDPARTS,
      ALTERTABLE_RENAMECOL, ALTERTABLE_REPLACECOLS, ALTERTABLE_DROPPARTS, ALTERTABLE_RENAMEPART,
      ALTERTABLE_RENAME, ALTERTABLE_PROPERTIES, ALTERTABLE_SERDEPROPERTIES, ALTERTABLE_LOCATION,
      ALTERVIEW_AS, ALTERVIEW_RENAME, ANALYZE_TABLE, CREATEDATABASE, CREATETABLE,
      CREATETABLE_AS_SELECT, CREATEFUNCTION, CREATEVIEW, DESCDATABASE, DESCFUNCTION, DESCTABLE,
      DROPDATABASE, DROPFUNCTION, DROPTABLE, DROPVIEW, EXPLAIN, LOAD, MSCK, QUERY, RELOADFUNCTION,
      SHOWCONF, SHOW_CREATETABLE, SHOWCOLUMNS, SHOWDATABASES, SHOWFUNCTIONS, SHOWPARTITIONS,
      SHOWTABLES, SHOW_TBLPROPERTIES, SWITCHDATABASE, TRUNCATETABLE = Value
}
