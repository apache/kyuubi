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
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class YarnCatalog extends TableCatalog with SupportsNamespaces with Logging {
  private var catalogName: String = _

  override def initialize(
      name: String,
      caseInsensitiveStringMap: CaseInsensitiveStringMap): Unit = {
    catalogName = name
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    Array(Identifier.of(namespace, "agg_logs"))
  }

  override def loadTable(identifier: Identifier): Table = ???

  override def createTable(
      identifier: Identifier,
      structType: StructType,
      transforms: Array[Transform],
      map: util.Map[String, String]): Table = ???

  override def alterTable(identifier: Identifier, tableChanges: TableChange*): Table = ???

  override def dropTable(identifier: Identifier): Boolean = ???

  override def renameTable(identifier: Identifier, identifier1: Identifier): Unit = ???

  override def listNamespaces(): Array[Array[String]] = ???

  override def listNamespaces(strings: Array[String]): Array[Array[String]] = ???

  override def loadNamespaceMetadata(strings: Array[String]): util.Map[String, String] = ???

  override def createNamespace(strings: Array[String], map: util.Map[String, String]): Unit = ???

  override def alterNamespace(strings: Array[String], namespaceChanges: NamespaceChange*): Unit =
    ???

  override def dropNamespace(strings: Array[String], b: Boolean): Boolean = ???

  override def name(): String = this.catalogName
}
