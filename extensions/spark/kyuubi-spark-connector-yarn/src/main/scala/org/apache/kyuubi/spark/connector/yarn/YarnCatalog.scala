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

import scala.jdk.CollectionConverters.mapAsJavaMapConverter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
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
    namespace(1) match {
      case "default" =>
        Array(Identifier.of(namespace, "app_logs"), Identifier.of(namespace, "apps"))
      case _ => throw new NoSuchNamespaceException(namespace)
    }
  }

  override def loadTable(identifier: Identifier): Table = identifier.name match {
    case "app_logs" => new YarnLogTable
    case "apps" => new YarnApplicationTable
    case _ => throw new NoSuchTableException(s"${identifier.name}")

  }

  override def createTable(
      identifier: Identifier,
      structType: StructType,
      transforms: Array[Transform],
      map: util.Map[String, String]): Table = {
    throw new UnsupportedOperationException("Create table is not supported")
  }

  override def alterTable(identifier: Identifier, tableChanges: TableChange*): Table = {
    throw new UnsupportedOperationException("Alter table is not supported")
  }

  override def dropTable(identifier: Identifier): Boolean = {
    throw new UnsupportedOperationException("Drop table is not supported")
  }

  override def renameTable(identifier: Identifier, identifier1: Identifier): Unit = {
    throw new UnsupportedOperationException("Rename table is not supported")
  }

  override def listNamespaces(): Array[Array[String]] = {
    Array(Array("default"))
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = namespace match {
    case Array() => listNamespaces()
    case Array(db) if db eq "default" => listNamespaces()
    case _ => throw new NoSuchNamespaceException(namespace)
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] =
    namespace match {
      case Array(_) => Map.empty[String, String].asJava
      case _ => throw new NoSuchNamespaceException(namespace)
    }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit =
    throw new UnsupportedOperationException

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit =
    throw new UnsupportedOperationException

  // Removed in SPARK-37929
  def dropNamespace(namespace: Array[String]): Boolean =
    throw new UnsupportedOperationException

  // Introduced in SPARK-37929
  def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean =
    throw new UnsupportedOperationException

  override def name(): String = this.catalogName
}
