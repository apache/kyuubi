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

import org.apache.kyuubi.plugin.spark.authz.serde.HarmlessNodeSpec

/**
 * The explicit allowlist of plan nodes that are not authorization-relevant, backing
 * `known_harmless_spec.json`. Every entry must carry a reason (an allowlist entry is a
 * reviewed security decision) and names the exact Spark minor versions the review was
 * performed against. On any other Spark version the entry is inert and the node counts as
 * unclassified: a class is free to change shape under the same fully qualified name in
 * the next release (exactly what CALL did between Spark 3 and 4), so re-reviewing this
 * list, entry by entry, is part of every Spark version bump.
 */
object KnownHarmlessNodes {

  // The Spark minors the plugin currently supports and tests per-profile. Every entry
  // below was reviewed against the 3.x baseline when the allowlist was introduced and
  // re-reviewed (and the exercised ones re-run in deny mode, full module suite per
  // profile) for the 4.x port. A version joins an entry's list by being tested or
  // reviewed, never by interpolation.
  private val spark3xAnd4x = Seq("3.3", "3.4", "3.5", "4.0", "4.1", "4.2")

  // Nodes that only exist on Spark 4.
  private val spark4x = Seq("4.0", "4.1", "4.2")

  val specs: Seq[HarmlessNodeSpec] = Seq(
    HarmlessNodeSpec(
      "org.apache.kyuubi.plugin.spark.authz.rule.rowfilter.FilteredShowColumnsCommand",
      "This plugin's own row-filtering replacement for ShowColumnsCommand (installed by" +
        " RuleReplaceShowObjectCommands); PrivilegesBuilder.build handles it with a" +
        " dedicated dispatch arm and every result row is checked for SHOWCOLUMNS access",
      spark3xAnd4x),
    HarmlessNodeSpec(
      "org.apache.kyuubi.plugin.spark.authz.rule.rowfilter.FilteredShowFunctionsCommand",
      "This plugin's own row-filtering replacement for ShowFunctionsCommand (installed by" +
        " RuleReplaceShowObjectCommands); PrivilegesBuilder.build handles it with a" +
        " dedicated dispatch arm and every result row is checked for SHOWFUNCTIONS access",
      spark3xAnd4x),
    HarmlessNodeSpec(
      "org.apache.kyuubi.plugin.spark.authz.rule.rowfilter.FilteredShowTablesCommand",
      "This plugin's own row-filtering replacement for ShowTablesCommand (installed by" +
        " RuleReplaceShowObjectCommands); PrivilegesBuilder.build handles it with a" +
        " dedicated dispatch arm and every result row is checked for SHOWTABLES access",
      spark3xAnd4x),
    HarmlessNodeSpec(
      "org.apache.spark.sql.catalyst.analysis.ResolvedNamespace",
      "Analysis-time resolution artifact naming a namespace; reads no data itself, and the" +
        " commands resolved over it are classified in their own right",
      spark3xAnd4x),
    HarmlessNodeSpec(
      "org.apache.spark.sql.catalyst.plans.logical.CTERelationRef",
      "Leaf reference to a CTE definition; the definition's own plan appears under" +
        " WithCTE in the same tree and is authorized there",
      spark3xAnd4x),
    HarmlessNodeSpec(
      "org.apache.spark.sql.catalyst.plans.logical.CommandResult",
      "Holds rows already produced by an eagerly executed command; that command was" +
        " authorized when it executed",
      spark3xAnd4x),
    HarmlessNodeSpec(
      "org.apache.spark.sql.catalyst.plans.logical.LocalRelation",
      "Holds in-memory literal rows (VALUES lists, createDataFrame); reads no stored data",
      spark3xAnd4x),
    HarmlessNodeSpec(
      "org.apache.spark.sql.catalyst.plans.logical.NoopCommand",
      "Spark's placeholder for commands with nothing to do (e.g. IF EXISTS / IF NOT EXISTS" +
        " variants when the object is absent); executes nothing",
      spark3xAnd4x),
    HarmlessNodeSpec(
      "org.apache.spark.sql.catalyst.plans.logical.OneRowRelation",
      "The implicit single-row relation backing SELECT without FROM; reads no stored data",
      spark3xAnd4x),
    HarmlessNodeSpec(
      "org.apache.spark.sql.catalyst.plans.logical.Range",
      "Generates rows from a numeric range (e.g. spark.range); reads no stored data",
      spark3xAnd4x),
    HarmlessNodeSpec(
      "org.apache.spark.sql.catalyst.plans.logical.ShowNamespaces",
      "Enforced elsewhere: results are row-filtered per namespace by ObjectFilterPlaceHolder" +
        " + FilterDataSourceV2Strategy; Spark eagerly executes the bare command in a nested" +
        " QueryExecution whose unfiltered result the placeholder discards",
      spark3xAnd4x),
    HarmlessNodeSpec(
      "org.apache.spark.sql.execution.command.ShowNamespacesCommand",
      "Enforced elsewhere: Spark 4.x's v1 SHOW DATABASES/NAMESPACES command; results are" +
        " row-filtered per namespace by ObjectFilterPlaceHolder + FilterDataSourceV2Strategy" +
        " exactly like v2 ShowNamespaces, and the bare command Spark eagerly executes in a" +
        " nested QueryExecution has its unfiltered result discarded by the placeholder",
      spark4x),
    HarmlessNodeSpec(
      "org.apache.spark.sql.catalyst.plans.logical.ShowTables",
      "Enforced elsewhere: results are row-filtered per table by ObjectFilterPlaceHolder" +
        " + FilterDataSourceV2Strategy; Spark eagerly executes the bare command in a nested" +
        " QueryExecution whose unfiltered result the placeholder discards",
      spark3xAnd4x),
    HarmlessNodeSpec(
      "org.apache.spark.sql.execution.ExternalRDD",
      "Wraps a session-created RDD/local collection (e.g. spark.createDataset, and Delta's" +
        " internal VACUUM plumbing); RDD-level access is outside the plugin's scope and is" +
        " an existing, separate concern",
      spark3xAnd4x),
    HarmlessNodeSpec(
      "org.apache.spark.sql.execution.LogicalRDD",
      "Wraps a pre-existing RDD; RDD-level access is outside the plugin's scope and is an" +
        " existing, separate concern",
      spark3xAnd4x),
    HarmlessNodeSpec(
      "org.apache.spark.sql.execution.columnar.InMemoryRelation",
      "Cached query results; the originating plan was authorized when the cache was" +
        " populated",
      spark3xAnd4x),
    HarmlessNodeSpec(
      "org.apache.spark.sql.execution.command.DropTempViewCommand",
      "Operates only on session-local temporary views, which are deliberately not authz" +
        " resources (their reads are authorized against the underlying tables); see" +
        " KYUUBI #3426",
      spark3xAnd4x),
    HarmlessNodeSpec(
      "org.apache.spark.sql.execution.command.ResetCommand",
      "Resets session configuration only; sensitive configs are separately guarded by" +
        " AuthzConfigurationChecker",
      spark3xAnd4x),
    HarmlessNodeSpec(
      "org.apache.spark.sql.execution.command.SetCommand",
      "Sets session configuration only; sensitive configs are separately guarded by" +
        " AuthzConfigurationChecker",
      spark3xAnd4x))
}
