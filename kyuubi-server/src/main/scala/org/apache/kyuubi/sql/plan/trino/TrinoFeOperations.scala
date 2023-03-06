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

package org.apache.kyuubi.sql.plan.trino

import org.apache.kyuubi.sql.plan.KyuubiTreeNode

/////////////////////////////////////////////////////////////////////////////////////////
// This file contains all Trino JDBC operation nodes which are parsed from statement
/////////////////////////////////////////////////////////////////////////////////////////

case class GetSchemas(catalogName: String, schemaPattern: String) extends KyuubiTreeNode {
  override def name(): String = "Get Schemas"
}

case class GetCatalogs() extends KyuubiTreeNode {
  override def name(): String = "Get Catalogs"
}

case class GetTableTypes() extends KyuubiTreeNode {
  override def name(): String = "Get Table Types"
}

case class GetTypeInfo() extends KyuubiTreeNode {
  override def name(): String = "Get Type Info"
}

case class GetTables(
    catalogName: String,
    schemaPattern: String,
    tableNamePattern: String,
    tableTypes: List[String],
    emptyResult: Boolean = false) extends KyuubiTreeNode {
  override def name(): String = "Get Tables"
}

case class GetColumns(
    catalogName: String,
    schemaPattern: String,
    tableNamePattern: String,
    colNamePattern: String) extends KyuubiTreeNode {
  override def name(): String = "Get Columns"
}

case class GetPrimaryKeys() extends KyuubiTreeNode {
  override def name(): String = "Get Primary Keys"
}
