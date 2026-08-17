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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.CachedData
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.command.ExplainCommand
import org.slf4j.LoggerFactory

import org.apache.kyuubi.plugin.spark.authz.OperationType.OperationType
import org.apache.kyuubi.plugin.spark.authz.ParanoidMode.ViolationKind
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectActionType._
import org.apache.kyuubi.plugin.spark.authz.rule.Authorization._
import org.apache.kyuubi.plugin.spark.authz.rule.rowfilter._
import org.apache.kyuubi.plugin.spark.authz.serde._
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.util.reflect.ReflectUtils._

object PrivilegesBuilder {

  final private val LOG = LoggerFactory.getLogger(getClass)

  private def collectLeaves(expr: Expression): Seq[NamedExpression] = {
    expr.collect { case p: NamedExpression if p.children.isEmpty => p }
  }

  private def setCurrentDBIfNecessary(
      tableIdent: Table,
      spark: SparkSession): Table = {
    if (tableIdent.database.isEmpty) {
      tableIdent.copy(database = Some(spark.catalog.currentDatabase))
    } else {
      tableIdent
    }
  }

  /**
   * Build PrivilegeObjects from Spark LogicalPlan
   *
   * @param plan a Spark LogicalPlan used to generate SparkPrivilegeObjects
   * @param privilegeObjects input or output spark privilege object list
   * @param projectionList Projection list after pruning
   */
  def buildQuery(
      plan: LogicalPlan,
      privilegeObjects: ArrayBuffer[PrivilegeObject],
      projectionList: Seq[NamedExpression] = Nil,
      conditionList: Seq[NamedExpression] = Nil,
      spark: SparkSession): Unit = {

    def mergeProjection(table: Table, plan: LogicalPlan): Unit = {
      if (projectionList.isEmpty) {
        privilegeObjects += PrivilegeObject(table, plan.output.map(_.name))
      } else {
        val cols = columnPrune(projectionList, plan.outputSet)
        privilegeObjects += PrivilegeObject(table, cols.map(_.name).distinct)
      }
    }

    def columnPrune(projectionList: Seq[Expression], output: AttributeSet): Seq[NamedExpression] = {
      (projectionList ++ conditionList)
        .flatMap(collectLeaves)
        .filter(output.contains)
    }

    plan match {
      case p if p.getTagValue(KYUUBI_AUTHZ_TAG).nonEmpty =>

      case scan if isKnownScan(scan) && scan.resolved =>
        val spec = getScanSpec(scan)
        val (tables, tableFailures) = spec.tablesWithFailures(scan, spark)
        // If the the scan is table-based, we check privileges on the table we found
        // otherwise, we check privileges on the uri we found
        if (tables.nonEmpty) {
          tables.foreach(mergeProjection(_, scan))
        } else {
          val (uris, uriFailures) = spec.urisWithFailures(scan)
          uris.foreach(privilegeObjects += PrivilegeObject(_))
          if (uris.isEmpty && (tableFailures.nonEmpty || uriFailures.nonEmpty)) {
            ParanoidMode.onViolation(
              spark,
              scan,
              ViolationKind.EXTRACTION_FAILURE,
              "scan spec matched but no table or uri could be extracted",
              (tableFailures ++ uriFailures).headOption)
          }
        }

      case u if u.nodeName == "UnresolvedRelation" =>
        val parts = invokeAs[String](u, "tableName").split("\\.")
        val db = quote(parts.init)
        val table = Table(None, Some(db), parts.last, None)
        privilegeObjects += PrivilegeObject(table)

      case cached: InMemoryRelation =>
        cachedQueryPlan(cached, spark) match {
          case Some(originalPlan) =>
            // Authorize the query the cache was built from, not the cache. Its own
            // projection and filters drive the column pruning, so the caller is asked for
            // exactly the columns that were materialized into this cache entry.
            buildQuery(originalPlan, privilegeObjects, spark = spark)
          case None =>
            ParanoidMode.onViolation(
              spark,
              cached,
              ViolationKind.EXTRACTION_FAILURE,
              "a cached relation was reached but the query it caches could not be" +
                " recovered from the CacheManager, so nothing constrains reading it")
        }

      case p =>
        checkUnclassifiedQueryNode(p, spark)
        for (child <- p.children) {
          // If current plan's references don't have relation to it's input, have two cases
          //   1. `MapInPandas`, `ScriptTransformation`
          //   2. `Project` output only have constant value
          if (columnPrune(p.references.toSeq ++ p.output, p.inputSet).isEmpty) {
            // If plan is project and output don't have relation to input, can ignore.
            if (!p.isInstanceOf[Project]) {
              buildQuery(
                child,
                privilegeObjects,
                p.inputSet.map(_.toAttribute).toSeq,
                Nil,
                spark)
            } else {
              // The subtree is pruned for privilege building (a constant projection reads
              // no columns), but it still executes, so classification must still see it:
              // an unclassified node must not hide under `SELECT <literal> FROM ...`.
              sweepClassificationOnly(child, spark)
            }
          } else {
            buildQuery(
              child,
              privilegeObjects,
              // Here we use `projectList ++ p.reference` do column prune.
              // For `Project`, `Aggregate`, plan's output is contained by plan's referenced
              // For `Filter`, `Sort` etc... it rely on upper `Project` node,
              //    since we wrap a `Project` before call `buildQuery()`.
              // So here we use upper node's projectionList and current's references
              // to do column pruning can get the correct column.
              columnPrune(projectionList ++ p.references.toSeq, p.inputSet).distinct,
              conditionList ++ p.references,
              spark)
          }
        }
    }
  }

  /**
   * Recover the query a cached relation stands for.
   *
   * `CacheManager.useCachedData` substitutes cached fragments before the optimizer runs,
   * and [[org.apache.kyuubi.plugin.spark.authz.ranger.RuleAuthorization]] is an optimizer
   * rule: by the time privileges are built, the relations the cached query read have
   * already collapsed into an opaque leaf. The CacheManager lives in `SharedState` and is
   * shared by every session in the engine, so treating that leaf as carrying no privileges
   * would let any user read any table that any other user had cached.
   *
   * The CacheManager still holds the analyzed plan each entry was built from. Entries are
   * matched on the cache builder rather than on the relation, because the relation handed
   * to the optimizer is a copy with its output re-mapped onto the fragment it replaced
   * (`InMemoryRelation.withOutput`), while the builder is carried over untouched.
   */
  private def cachedQueryPlan(
      cached: InMemoryRelation,
      spark: SparkSession): Option[LogicalPlan] = {
    val entries =
      try {
        // CacheManager exposes lookup only by plan, and the plan is what we are missing
        getField[Seq[CachedData]](spark.sharedState.cacheManager, "cachedData")
      } catch {
        // ReflectUtils reports every reflective failure as RuntimeException; on a Spark
        // whose CacheManager no longer holds this field the caller fails closed instead
        case e: RuntimeException =>
          LOG.debug("Could not read CacheManager.cachedData", e)
          return None
      }
    entries.collectFirst {
      case entry if entry.cachedRepresentation.cacheBuilder eq cached.cacheBuilder => entry.plan
    }
  }

  /**
   * Classification checks for nodes on the query path. Ordinary non-leaf operators
   * (Project, Filter, Join, ...) recurse freely (privileges are carried by leaves and
   * commands) but a node matching any of the shapes below would otherwise be silently
   * treated as not-authz-relevant, and [[ParanoidMode]] decides how loud that is.
   *
   * If the class has an allowlist entry that simply is not verified for the running Spark
   * version, say so: "re-review the entry" is far more actionable than "unclassified".
   */
  private def unverifiedAllowlistDetail(classname: String): String = {
    KNOWN_HARMLESS_NODES.get(classname).map { spec =>
      s"""its known_harmless_spec.json entry is verified for
         | Spark ${spec.verifiedSparkVersions.mkString(", ")}
         | but not for $SPARK_RUNTIME_MAJOR_MINOR -
         | re-review the entry for this version""".stripMargin
    }.getOrElse("")
  }

  private def checkUnclassifiedQueryNode(p: LogicalPlan, spark: SparkSession): Unit = {
    if (isKnownHarmless(p)) {
      return
    }
    val detail = unverifiedAllowlistDetail(p.getClass.getName)
    if (hasCommandSpec(p.getClass.getName)) {
      // The spec exists but dispatch never consulted it — the class kept its name but
      // changed supertype, like CALL between Spark 3 (Iceberg's Command) and Spark 4.
      ParanoidMode.onViolation(spark, p, ViolationKind.UNREACHABLE_SPEC, detail)
    } else if (executesDuringAnalysis(p)) {
      ParanoidMode.onViolation(spark, p, ViolationKind.ANALYSIS_TIME_EXECUTION, detail)
    } else if (isKnownScan(p)) {
      // a known scan only reaches the generic arm when unresolved, contributing nothing
      ParanoidMode.onViolation(spark, p, ViolationKind.UNRESOLVED_SCAN, detail)
    } else if (p.children.isEmpty) {
      ParanoidMode.onViolation(spark, p, ViolationKind.UNCLASSIFIED_LEAF, detail)
    }
  }

  /**
   * Walk a subtree that privilege building skips, applying only the classification checks.
   * Contributes no privilege objects; nodes are classified and traversal pruned exactly as
   * [[buildQuery]] would (no descent below checked, scan, or unresolved-relation nodes).
   */
  private def sweepClassificationOnly(plan: LogicalPlan, spark: SparkSession): Unit = {
    plan match {
      case p if p.getTagValue(KYUUBI_AUTHZ_TAG).nonEmpty =>
      case scan if isKnownScan(scan) && scan.resolved =>
      case u if u.nodeName == "UnresolvedRelation" =>
      // buildQuery has a dedicated arm for cached relations; under a constant projection
      // no column of the cache is read, exactly as for a scan
      case _: InMemoryRelation =>
      case p =>
        checkUnclassifiedQueryNode(p, spark)
        p.children.foreach(sweepClassificationOnly(_, spark))
    }
  }

  /**
   * Tracks descriptor outcomes across all families (table/database/uri/query/function) of
   * one matched command spec. Individual descriptors failing is expected version variance:
   * specs are written so an object "wins at least once" across Spark versions and command
   * shapes (e.g. table descs legitimately all fail for a path-based procedure whose uri
   * descs succeed). What must not pass silently is the drift shape where *no* descriptor of
   * the command completes at all: the spec matches by name but can no longer extract
   * anything from this Spark's plan shape (fail-open layer 3).
   */
  private class DescOutcomes(plan: LogicalPlan, spark: SparkSession) {
    private var succeeded = 0
    private val failures = ArrayBuffer[Exception]()

    def run[D <: Descriptor](descs: Seq[D])(run: D => Unit): Unit = {
      descs.foreach { d =>
        try {
          run(d)
          succeeded += 1
        } catch {
          // an authorization decision bubbling up from nested privilege building is a
          // verdict, not an extraction failure — never swallow it
          case e: AccessControlException => throw e
          case e: Exception =>
            LOG.debug(d.error(plan, e))
            failures += e
        }
      }
    }

    def reportIfAllFailed(): Unit = {
      if (succeeded == 0 && failures.nonEmpty) {
        ParanoidMode.onViolation(
          spark,
          plan,
          ViolationKind.EXTRACTION_FAILURE,
          "no descriptor of the matched command spec completed extraction",
          failures.headOption)
      }
    }
  }

  /**
   * Build PrivilegeObjects from Spark LogicalPlan
   * @param plan a Spark LogicalPlan used to generate Spark PrivilegeObjects
   * @param inputObjs input privilege object list
   * @param outputObjs output privilege object list
   */
  private def buildCommand(
      plan: LogicalPlan,
      inputObjs: ArrayBuffer[PrivilegeObject],
      outputObjs: ArrayBuffer[PrivilegeObject],
      spark: SparkSession): OperationType = {

    def getTablePriv(tableDesc: TableDesc): Seq[PrivilegeObject] = {
      val maybeTable = tableDesc.extract(plan, spark)
      maybeTable match {
        case Some(table) =>
          val newTable = if (tableDesc.setCurrentDatabaseIfMissing) {
            setCurrentDBIfNecessary(table, spark)
          } else {
            table
          }
          if (tableDesc.tableTypeDesc.exists(_.skip(plan))) {
            Nil
          } else {
            val actionType = tableDesc.actionTypeDesc.map(_.extract(plan)).getOrElse(OTHER)
            val columnNames = tableDesc.columnDesc.map(_.extract(plan)).getOrElse(Nil)
            Seq(PrivilegeObject(newTable, columnNames, actionType))
          }
        case None => Nil
      }
    }

    plan.getClass.getName match {
      case classname if DB_COMMAND_SPECS.contains(classname) =>
        val desc = DB_COMMAND_SPECS(classname)
        val outcomes = new DescOutcomes(plan, spark)
        outcomes.run(desc.databaseDescs) { databaseDesc =>
          val database = databaseDesc.extract(plan)
          if (databaseDesc.isInput) {
            inputObjs += PrivilegeObject(database)
          } else {
            outputObjs += PrivilegeObject(database)
          }
        }
        outcomes.run(desc.uriDescs) { ud =>
          val uris = ud.extract(plan, spark)
          if (ud.isInput) {
            inputObjs ++= uris.map(PrivilegeObject(_))
          } else {
            outputObjs ++= uris.map(PrivilegeObject(_))
          }
        }
        outcomes.reportIfAllFailed()
        desc.operationType

      case classname if TABLE_COMMAND_SPECS.contains(classname) =>
        val spec = TABLE_COMMAND_SPECS(classname)
        val outcomes = new DescOutcomes(plan, spark)
        outcomes.run(spec.tableDescs) { td =>
          if (td.isInput) {
            inputObjs ++= getTablePriv(td)
          } else {
            outputObjs ++= getTablePriv(td)
          }
        }
        outcomes.run(spec.uriDescs) { ud =>
          val uris = ud.extract(plan, spark)
          if (ud.isInput) {
            inputObjs ++= uris.map(PrivilegeObject(_))
          } else {
            outputObjs ++= uris.map(PrivilegeObject(_))
          }
        }
        // extract inside the tracked run (extraction failures are layer 3), but recurse
        // into the extracted queries outside it, so their violations surface as themselves
        val queries = ArrayBuffer[LogicalPlan]()
        outcomes.run(spec.queryDescs) { qd =>
          queries ++= qd.extract(plan)
        }
        outcomes.reportIfAllFailed()
        queries.foreach { p =>
          buildQuery(Project(p.output, p), inputObjs, spark = spark)
        }
        spec.operationType

      case classname if FUNCTION_COMMAND_SPECS.contains(classname) =>
        val spec = FUNCTION_COMMAND_SPECS(classname)
        val outcomes = new DescOutcomes(plan, spark)
        outcomes.run(spec.functionDescs) { fd =>
          val function = fd.extract(plan)
          if (!fd.functionTypeDesc.exists(_.skip(plan, spark))) {
            if (fd.isInput) {
              inputObjs += PrivilegeObject(function)
            } else {
              outputObjs += PrivilegeObject(function)
            }
          }
        }
        outcomes.reportIfAllFailed()
        spec.operationType

      case classname =>
        // fail-open layer 1: a command with no spec produces zero access requests
        if (!isKnownHarmlessClassname(classname)) {
          ParanoidMode.onViolation(
            spark,
            plan,
            ViolationKind.UNCLASSIFIED_COMMAND,
            unverifiedAllowlistDetail(classname))
        }
        OperationType.QUERY
    }
  }

  type PrivilegesAndOpType = (Iterable[PrivilegeObject], Iterable[PrivilegeObject], OperationType)

  /**
   * Build input  privilege objects from a Spark's LogicalPlan for hive permanent udf
   *
   * @param plan      A Spark LogicalPlan
   */
  def buildFunctions(
      plan: LogicalPlan,
      spark: SparkSession): PrivilegesAndOpType = {
    val inputObjs = new ArrayBuffer[PrivilegeObject]
    plan match {
      case command: Command if isKnownTableCommand(command) =>
        val spec = getTableCommandSpec(command)
        val functionPrivAndOpType = spec.queries(plan)
          .map(plan => buildFunctions(plan, spark))
        inputObjs ++= functionPrivAndOpType.flatMap(_._1)

      case plan => plan transformAllExpressions {
          case hiveFunction: Expression if isKnownFunction(hiveFunction) =>
            val functionSpec: ScanSpec = getFunctionSpec(hiveFunction)
            if (functionSpec.functionDescs
                .exists(!_.functionTypeDesc.get.skip(hiveFunction, spark))) {
              functionSpec.functions(hiveFunction).foreach(func =>
                inputObjs += PrivilegeObject(func))
            }
            hiveFunction
        }
    }
    (inputObjs, Seq.empty, OperationType.QUERY)
  }

  /**
   * Build input and output privilege objects from a Spark's LogicalPlan
   *
   * For `Command`s, build outputs if it has an target to write, build inputs for the
   * inside query if exists.
   *
   * For other queries, build inputs.
   *
   * @param plan A Spark LogicalPlan
   */
  def build(
      plan: LogicalPlan,
      spark: SparkSession): PrivilegesAndOpType = {
    // Spark 4.0 wraps eagerly-executed commands (e.g. CALL) in CommandResult; unwrap so the
    // underlying command is authorized and its privileges are extracted.
    val plan0 = unwrapCommandResult(plan)
    val inputObjs = new ArrayBuffer[PrivilegeObject]
    val outputObjs = new ArrayBuffer[PrivilegeObject]
    val opType = plan0 match {
      case ObjectFilterPlaceHolder(child) if child.nodeName == "ShowTables" =>
        OperationType.SHOWTABLES
      case ObjectFilterPlaceHolder(child) if child.nodeName == "ShowNamespaces" =>
        OperationType.SHOWDATABASES
      case _: FilteredShowTablesCommand => OperationType.SHOWTABLES
      case _: FilteredShowFunctionsCommand => OperationType.SHOWFUNCTIONS
      case _: FilteredShowColumnsCommand => OperationType.SHOWCOLUMNS

      // ExplainCommand run will execute the plan, should avoid check privilege for the plan.
      case _: ExplainCommand =>
        setExplainCommandExecutionId(spark)
        OperationType.EXPLAIN
      case _ if isExplainCommandChild(spark) =>
        OperationType.EXPLAIN

      // RunnableCommand
      case cmd: Command => buildCommand(cmd, inputObjs, outputObjs, spark)

      // A node with a command spec that is not a Command on this Spark version: the class
      // kept its name but changed supertype (e.g. CALL, a Command via Iceberg on Spark 3
      // but an ExecutableDuringAnalysis UnaryNode on Spark 4). Route it to its spec instead
      // of letting it fall through to the query path where the spec is unreachable.
      case cmd if hasCommandSpec(cmd.getClass.getName) =>
        buildCommand(cmd, inputObjs, outputObjs, spark)
      // Queries
      case _ =>
        buildQuery(Project(plan0.output, plan0), inputObjs, spark = spark)
        OperationType.QUERY
    }
    (inputObjs, outputObjs, opType)
  }
}
