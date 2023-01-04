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

package org.apache.kyuubi.plugin.spark.authz.gen

import org.apache.kyuubi.plugin.spark.authz.serde._
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.extractorName

object DatabaseCommands {

  val AlterDatabaseProperties = {
    DatabaseCommandSpec(
      "org.apache.spark.sql.execution.command.AlterDatabasePropertiesCommand",
      Seq(DatabaseDesc("databaseName", extractorName[StringDatabaseExtractor])),
      "ALTERDATABASE")
  }

  val CommentOnNamespace = {
    DatabaseCommandSpec(
      "org.apache.spark.sql.catalyst.plans.logical.CommentOnNamespace",
      Seq(DatabaseDesc("child", extractorName[ResolvedNamespaceDatabaseExtractor])),
      "ALTERDATABASE")
  }

  val SetNamespaceProperties = {
    DatabaseCommandSpec(
      "org.apache.spark.sql.catalyst.plans.logical.SetNamespaceProperties",
      Seq(DatabaseDesc("namespace", extractorName[ResolvedNamespaceDatabaseExtractor])),
      "ALTERDATABASE")
  }

  val SetNamespaceLocation = {
    DatabaseCommandSpec(
      "org.apache.spark.sql.catalyst.plans.logical.SetNamespaceLocation",
      Seq(DatabaseDesc("namespace", extractorName[ResolvedNamespaceDatabaseExtractor])),
      "ALTERDATABASE_LOCATION")
  }

  val CreateNamespace = {
    val databaseDesc1 =
      DatabaseDesc("name", extractorName[ResolvedDBObjectNameDatabaseExtractor])
    val databaseDesc2 =
      DatabaseDesc(
        "namespace",
        extractorName[StringSeqDatabaseExtractor],
        catalogDesc = Some(CatalogDesc()))
    DatabaseCommandSpec(
      "org.apache.spark.sql.catalyst.plans.logical.CreateNamespace",
      Seq(databaseDesc1, databaseDesc2),
      "CREATEDATABASE")
  }

  val DropNamespace = {
    DatabaseCommandSpec(
      "org.apache.spark.sql.catalyst.plans.logical.DropNamespace",
      Seq(DatabaseDesc("namespace", extractorName[ResolvedNamespaceDatabaseExtractor])),
      "DROPDATABASE")
  }

  val AnalyzeTables = {
    DatabaseCommandSpec(
      "org.apache.spark.sql.execution.command.AnalyzeTablesCommand",
      Seq(DatabaseDesc(
        "databaseName",
        extractorName[StringOptionDatabaseExtractor],
        isInput = true)),
      "ANALYZE_TABLE")
  }

  val SetDatabase = {
    val cmd = "org.apache.spark.sql.execution.command.SetDatabaseCommand"
    val databaseDesc =
      DatabaseDesc("databaseName", extractorName[StringDatabaseExtractor], isInput = true)
    DatabaseCommandSpec(cmd, Seq(databaseDesc), "SWITCHDATABASE")
  }

  val DescribeDatabase = {
    val cmd = "org.apache.spark.sql.execution.command.DescribeDatabaseCommand"
    val databaseDesc =
      DatabaseDesc("databaseName", extractorName[StringDatabaseExtractor], isInput = true)
    DatabaseCommandSpec(cmd, Seq(databaseDesc), "DESCDATABASE")
  }

  val SetCatalogAndNamespace = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.SetCatalogAndNamespace"
    val databaseDesc1 =
      DatabaseDesc(
        "child",
        extractorName[ResolvedDBObjectNameDatabaseExtractor],
        isInput = true)
    val databaseDesc2 =
      DatabaseDesc(
        "namespace",
        extractorName[StringSeqOptionDatabaseExtractor],
        catalogDesc = Some(CatalogDesc(
          fieldName = "catalogName",
          fieldExtractor = extractorName[StringOptionCatalogExtractor])),
        isInput = true)
    DatabaseCommandSpec(cmd, Seq(databaseDesc1, databaseDesc2), "SWITCHDATABASE")
  }

  val SetNamespace = {
    val cmd = "org.apache.spark.sql.execution.command.SetNamespaceCommand"
    val databaseDesc = DatabaseDesc(
      "namespace",
      extractorName[StringSeqDatabaseExtractor],
      isInput = true)
    DatabaseCommandSpec(cmd, Seq(databaseDesc), "SWITCHDATABASE")
  }

  val DescribeNamespace = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.DescribeNamespace"
    val databaseDesc =
      DatabaseDesc(
        "namespace",
        extractorName[ResolvedNamespaceDatabaseExtractor],
        isInput = true)
    DatabaseCommandSpec(cmd, Seq(databaseDesc), "DESCDATABASE")
  }

  val data = Array(
    AlterDatabaseProperties,
    AlterDatabaseProperties.copy(
      classname = "org.apache.spark.sql.execution.command.AlterDatabaseSetLocationCommand",
      opType = "ALTERDATABASE_LOCATION"),
    AlterDatabaseProperties.copy(
      classname = "org.apache.spark.sql.execution.command.CreateDatabaseCommand",
      opType = "CREATEDATABASE"),
    AlterDatabaseProperties.copy(
      classname = "org.apache.spark.sql.execution.command.DropDatabaseCommand",
      opType = "DROPDATABASE"),
    AnalyzeTables,
    CreateNamespace,
    CommentOnNamespace,
    DescribeDatabase,
    DescribeNamespace,
    DropNamespace,
    SetCatalogAndNamespace,
    SetDatabase,
    SetNamespace,
    SetNamespaceProperties,
    SetNamespaceLocation)
}
