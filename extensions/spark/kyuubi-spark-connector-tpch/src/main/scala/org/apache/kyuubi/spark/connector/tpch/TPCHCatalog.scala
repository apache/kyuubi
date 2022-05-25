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

package org.apache.kyuubi.spark.connector.tpch

import java.util

import scala.collection.JavaConverters._

import io.trino.tpch.TpchTable
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog.{Identifier, Table => SparkTable, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class TPCHCatalog extends TableCatalog {

  val tables: Array[String] = TpchTable.getTables.asScala
    .map(_.getTableName).toArray

  val scales: Array[Int] = TPCHStatisticsUtils.SCALES

  val databases: Array[String] = scales.map("sf" + _)

  var options: CaseInsensitiveStringMap = _

  var _name: Option[String] = None

  override def name: String = _name.getOrElse("tpch")

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.options = options
    this._name = Some(name)
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = namespace match {
    case Array(db) if databases contains db => tables.map(Identifier.of(namespace, _))
    case _ => throw new NoSuchNamespaceException(namespace.mkString("."))
  }

  override def loadTable(ident: Identifier): SparkTable = (ident.namespace, ident.name) match {
    case (Array(db), table) if (databases contains db) && tables.contains(table.toLowerCase) =>
      new TPCHTable(table.toLowerCase, scales(databases indexOf db), options)
    case (_, _) => throw new NoSuchTableException(ident)
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): SparkTable =
    throw new UnsupportedOperationException

  override def alterTable(ident: Identifier, changes: TableChange*): SparkTable =
    throw new UnsupportedOperationException

  override def dropTable(ident: Identifier): Boolean =
    throw new UnsupportedOperationException

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new UnsupportedOperationException

}
