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

package org.apache.kyuubi.spark.connector.yarn

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class YarnCatalog extends TableCatalog with Logging {
  private var catalogName: String = _

  override def name: String = this.catalogName

  override def initialize(
      name: String,
      caseInsensitiveStringMap: CaseInsensitiveStringMap): Unit = {
    catalogName = name
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = namespace match {
    case Array() => Array(Identifier.of(namespace, "app_logs"))
    case _ => Array.empty
  }

  override def loadTable(identifier: Identifier): Table = identifier.name match {
    case "app_logs" if identifier.namespace().isEmpty => new YarnLogTable
    case _ => throw new NoSuchTableException(identifier)
  }

  override def createTable(
      identifier: Identifier,
      structType: StructType,
      transforms: Array[Transform],
      map: util.Map[String, String]): Table = {
    throw new UnsupportedOperationException()
  }

  override def alterTable(identifier: Identifier, tableChanges: TableChange*): Table = {
    throw new UnsupportedOperationException()
  }

  override def dropTable(identifier: Identifier): Boolean = {
    throw new UnsupportedOperationException()
  }

  override def renameTable(identifier: Identifier, identifier1: Identifier): Unit = {
    throw new UnsupportedOperationException()
  }
}
