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

package org.apache.kyuubi.plugin.lineage.helper

import scala.collection.immutable.ListMap
import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.kyuubi.lineage.{LineageConf, SparkContextHelper}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NamedRelation, PersistedView, ViewType}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, Expression, NamedExpression, ScalarSubquery}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, TableCatalog}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}

import org.apache.kyuubi.plugin.lineage.Lineage
import org.apache.kyuubi.plugin.lineage.helper.SparkListenerHelper.SPARK_RUNTIME_VERSION
import org.apache.kyuubi.util.reflect.ReflectUtils._

trait LineageParser {
  def sparkSession: SparkSession

  val SUBQUERY_COLUMN_IDENTIFIER = "__subquery__"
  val AGGREGATE_COUNT_COLUMN_IDENTIFIER = "__count__"
  val LOCAL_TABLE_IDENTIFIER = "__local__"

  type AttributeMap[A] = ListMap[Attribute, A]

  def parse(plan: LogicalPlan): Lineage = {
    val columnsLineage =
      extractColumnsLineage(plan, ListMap[Attribute, AttributeSet]()).toList.collect {
        case (k, attrs) =>
          k.name -> attrs.map(attr => (attr.qualifier :+ attr.name).mkString(".")).toSet
      }
    val (inputTables, outputTables) = columnsLineage.foldLeft((List[String](), List[String]())) {
      case ((inputs, outputs), (out, in)) =>
        val x = (inputs ++ in.map(_.split('.').init.mkString("."))).filter(_.nonEmpty)
        val y = outputs ++ List(out.split('.').init.mkString(".")).filter(_.nonEmpty)
        (x, y)
    }
    Lineage(inputTables.distinct, outputTables.distinct, columnsLineage)
  }

  private def mergeColumnsLineage(
      left: AttributeMap[AttributeSet],
      right: AttributeMap[AttributeSet]): AttributeMap[AttributeSet] = {
    left ++ right.map {
      case (k, attrs) =>
        k -> (attrs ++ left.getOrElse(k, AttributeSet.empty))
    }
  }

  private def joinColumnsLineage(
      parent: AttributeMap[AttributeSet],
      child: AttributeMap[AttributeSet]): AttributeMap[AttributeSet] = {
    if (parent.isEmpty) child
    else {
      val childMap = child.map { case (k, attrs) => (k.exprId, attrs) }
      parent.map { case (k, attrs) =>
        k -> AttributeSet(attrs.flatMap(attr =>
          childMap.getOrElse(
            attr.exprId,
            if (attr.name.equalsIgnoreCase(AGGREGATE_COUNT_COLUMN_IDENTIFIER)) AttributeSet(attr)
            else AttributeSet.empty)))
      }
    }
  }

  private def getExpressionSubqueryPlans(expression: Expression): Seq[LogicalPlan] = {
    expression match {
      case s: ScalarSubquery => Seq(s.plan)
      case s => s.children.flatMap(getExpressionSubqueryPlans)
    }
  }

  private def findSparkPlanLogicalLink(sparkPlans: Seq[SparkPlan]): Option[LogicalPlan] = {
    sparkPlans.find(_.logicalLink.nonEmpty) match {
      case Some(sparkPlan) => sparkPlan.logicalLink
      case None => findSparkPlanLogicalLink(sparkPlans.flatMap(_.children))
    }
  }

  private def containsCountAll(expr: Expression): Boolean = {
    expr match {
      case e: Count if e.references.isEmpty => true
      case e =>
        e.children.exists(containsCountAll)
    }
  }

  private def getSelectColumnLineage(
      named: Seq[NamedExpression]): AttributeMap[AttributeSet] = {
    val exps = named.map {
      case exp: Alias =>
        val references =
          if (exp.references.nonEmpty) exp.references
          else {
            val attrRefs = getExpressionSubqueryPlans(exp.child)
              .map(extractColumnsLineage(_, ListMap[Attribute, AttributeSet]()))
              .foldLeft(ListMap[Attribute, AttributeSet]())(mergeColumnsLineage).values
              .foldLeft(AttributeSet.empty)(_ ++ _)
              .map(attr => attr.withQualifier(attr.qualifier :+ SUBQUERY_COLUMN_IDENTIFIER))
            AttributeSet(attrRefs)
          }
        (
          exp.toAttribute,
          if (!containsCountAll(exp.child)) references
          else references + exp.toAttribute.withName(AGGREGATE_COUNT_COLUMN_IDENTIFIER))
      case a: Attribute => a -> AttributeSet(a)
    }
    ListMap(exps: _*)
  }

  private def joinRelationColumnLineage(
      parent: AttributeMap[AttributeSet],
      relationAttrs: Seq[Attribute],
      qualifier: Seq[String]): AttributeMap[AttributeSet] = {
    val relationAttrSet = AttributeSet(relationAttrs)
    if (parent.nonEmpty) {
      parent.map { case (k, attrs) =>
        k -> AttributeSet(attrs.collect {
          case attr if relationAttrSet.contains(attr) =>
            attr.withQualifier(qualifier)
          case attr
              if attr.qualifier.nonEmpty && attr.qualifier.last.equalsIgnoreCase(
                SUBQUERY_COLUMN_IDENTIFIER) =>
            attr.withQualifier(attr.qualifier.init)
          case attr if attr.name.equalsIgnoreCase(AGGREGATE_COUNT_COLUMN_IDENTIFIER) =>
            attr.withQualifier(qualifier)
          case attr if isNameWithQualifier(attr, qualifier) =>
            val newName = attr.name.split('.').last.stripPrefix("`").stripSuffix("`")
            attr.withName(newName).withQualifier(qualifier)
        })
      }
    } else {
      ListMap(relationAttrs.map { attr =>
        (
          attr,
          AttributeSet(attr.withQualifier(qualifier)))
      }: _*)
    }
  }

  private def isNameWithQualifier(attr: Attribute, qualifier: Seq[String]): Boolean = {
    val nameTokens = attr.name.split('.')
    val namespace = nameTokens.init.mkString(".")
    nameTokens.length > 1 && namespace.endsWith(qualifier.mkString("."))
  }

  private def mergeRelationColumnLineage(
      parentColumnsLineage: AttributeMap[AttributeSet],
      relationOutput: Seq[Attribute],
      relationColumnLineage: AttributeMap[AttributeSet]): AttributeMap[AttributeSet] = {
    val mergedRelationColumnLineage = {
      relationOutput.foldLeft((ListMap[Attribute, AttributeSet](), relationColumnLineage)) {
        case ((acc, x), attr) if x.isEmpty =>
          (acc + (attr -> AttributeSet.empty), x.empty)
        case ((acc, x), attr) =>
          (acc + (attr -> x.head._2), x.tail)
      }._1
    }
    joinColumnsLineage(parentColumnsLineage, mergedRelationColumnLineage)
  }

  private def extractColumnsLineage(
      plan: LogicalPlan,
      parentColumnsLineage: AttributeMap[AttributeSet]): AttributeMap[AttributeSet] = {

    plan match {
      // For command
      case p if p.nodeName == "CommandResult" =>
        val commandPlan = getField[LogicalPlan](plan, "commandLogicalPlan")
        extractColumnsLineage(commandPlan, parentColumnsLineage)
      case p if p.nodeName == "AlterViewAsCommand" =>
        val query =
          if (SPARK_RUNTIME_VERSION <= "3.1") {
            sparkSession.sessionState.analyzer.execute(getQuery(plan))
          } else {
            getQuery(plan)
          }
        val view = getV1TableName(getField[TableIdentifier](plan, "name").unquotedString)
        extractColumnsLineage(query, parentColumnsLineage).map { case (k, v) =>
          k.withName(s"$view.${k.name}") -> v
        }

      case p
          if p.nodeName == "CreateViewCommand"
            && getField[ViewType](plan, "viewType") == PersistedView =>
        val view = getV1TableName(getField[TableIdentifier](plan, "name").unquotedString)
        val outputCols =
          getField[Seq[(String, Option[String])]](plan, "userSpecifiedColumns").map(_._1)
        val query =
          if (SPARK_RUNTIME_VERSION <= "3.1") {
            sparkSession.sessionState.analyzer.execute(getField[LogicalPlan](plan, "child"))
          } else {
            getField[LogicalPlan](plan, "plan")
          }

        val lineages = extractColumnsLineage(query, parentColumnsLineage).zipWithIndex.map {
          case ((k, v), i) if outputCols.nonEmpty => k.withName(s"$view.${outputCols(i)}") -> v
          case ((k, v), _) => k.withName(s"$view.${k.name}") -> v
        }.toSeq
        ListMap[Attribute, AttributeSet](lineages: _*)

      case p if p.nodeName == "CreateDataSourceTableAsSelectCommand" =>
        val table = getV1TableName(getField[CatalogTable](plan, "table").qualifiedName)
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map { case (k, v) =>
          k.withName(s"$table.${k.name}") -> v
        }

      case p
          if p.nodeName == "CreateHiveTableAsSelectCommand" ||
            p.nodeName == "OptimizedCreateHiveTableAsSelectCommand" =>
        val table = getV1TableName(getField[CatalogTable](plan, "tableDesc").qualifiedName)
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map { case (k, v) =>
          k.withName(s"$table.${k.name}") -> v
        }

      case p
          if p.nodeName == "CreateTableAsSelect" ||
            p.nodeName == "ReplaceTableAsSelect" =>
        val (table, namespace, catalog) =
          if (SPARK_RUNTIME_VERSION <= "3.2") {
            (
              getField[Identifier](plan, "tableName").name,
              getField[Identifier](plan, "tableName").namespace.mkString("."),
              getField[TableCatalog](plan, "catalog").name())
          } else {
            (
              invokeAs[Identifier](plan, "tableName").name(),
              invokeAs[Identifier](plan, "tableName").namespace().mkString("."),
              getField[CatalogPlugin](
                invokeAs[LogicalPlan](plan, "name"),
                "catalog").name())
          }
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map { case (k, v) =>
          k.withName(Seq(catalog, namespace, table, k.name).filter(_.nonEmpty).mkString(".")) -> v
        }

      case p if p.nodeName == "InsertIntoDataSourceCommand" =>
        val logicalRelation = getField[LogicalRelation](plan, "logicalRelation")
        val table = logicalRelation
          .catalogTable.map(t => getV1TableName(t.qualifiedName)).getOrElse("")
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map {
          case (k, v) if table.nonEmpty =>
            k.withName(s"$table.${k.name}") -> v
        }

      case p if p.nodeName == "InsertIntoHadoopFsRelationCommand" =>
        val table =
          getField[Option[CatalogTable]](plan, "catalogTable")
            .map(t => getV1TableName(t.qualifiedName))
            .getOrElse("")
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map {
          case (k, v) if table.nonEmpty =>
            k.withName(s"$table.${k.name}") -> v
        }

      case p
          if p.nodeName == "InsertIntoDataSourceDirCommand" ||
            p.nodeName == "InsertIntoHiveDirCommand" =>
        val dir =
          getField[CatalogStorageFormat](plan, "storage").locationUri.map(_.toString)
            .getOrElse("")
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map {
          case (k, v) if dir.nonEmpty =>
            k.withName(s"`$dir`.${k.name}") -> v
        }

      case p if p.nodeName == "InsertIntoHiveTable" =>
        val table = getV1TableName(getField[CatalogTable](plan, "table").qualifiedName)
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map { case (k, v) =>
          k.withName(s"$table.${k.name}") -> v
        }

      case p if p.nodeName == "SaveIntoDataSourceCommand" =>
        extractColumnsLineage(getQuery(plan), parentColumnsLineage)

      case p
          if p.nodeName == "AppendData"
            || p.nodeName == "OverwriteByExpression"
            || p.nodeName == "OverwritePartitionsDynamic" =>
        val table = getV2TableName(getField[NamedRelation](plan, "table"))
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map { case (k, v) =>
          k.withName(s"$table.${k.name}") -> v
        }

      case p if p.nodeName == "MergeIntoTable" =>
        val matchedActions = getField[Seq[MergeAction]](plan, "matchedActions")
        val notMatchedActions = getField[Seq[MergeAction]](plan, "notMatchedActions")
        val allAssignments = (matchedActions ++ notMatchedActions).collect {
          case UpdateAction(_, assignments) => assignments
          case InsertAction(_, assignments) => assignments
        }.flatten
        val nextColumnsLlineage = ListMap(allAssignments.map { assignment =>
          (
            assignment.key.asInstanceOf[Attribute],
            assignment.value.references)
        }: _*)
        val targetTable = getField[LogicalPlan](plan, "targetTable")
        val sourceTable = getField[LogicalPlan](plan, "sourceTable")
        val targetColumnsLineage = extractColumnsLineage(
          targetTable,
          nextColumnsLlineage.map { case (k, _) => (k, AttributeSet(k)) })
        val sourceColumnsLineage = extractColumnsLineage(sourceTable, nextColumnsLlineage)
        val targetColumnsWithTargetTable = targetColumnsLineage.values.flatten.map { column =>
          val unquotedQualifiedName = (column.qualifier :+ column.name).mkString(".")
          column.withName(unquotedQualifiedName)
        }
        ListMap(targetColumnsWithTargetTable.zip(sourceColumnsLineage.values).toSeq: _*)

      case p if p.nodeName == "WithCTE" =>
        val optimized = sparkSession.sessionState.optimizer.execute(p)
        extractColumnsLineage(optimized, parentColumnsLineage)

      // For query
      case p: Project =>
        val nextColumnsLineage =
          joinColumnsLineage(parentColumnsLineage, getSelectColumnLineage(p.projectList))
        p.children.map(extractColumnsLineage(_, nextColumnsLineage)).reduce(mergeColumnsLineage)

      case p: Aggregate =>
        val nextColumnsLineage =
          joinColumnsLineage(parentColumnsLineage, getSelectColumnLineage(p.aggregateExpressions))
        p.children.map(extractColumnsLineage(_, nextColumnsLineage)).reduce(mergeColumnsLineage)

      case p: Expand =>
        val references =
          p.projections.transpose.map(_.flatMap(x => x.references)).map(AttributeSet(_))

        val childColumnsLineage = ListMap(p.output.zip(references): _*)
        val nextColumnsLineage =
          joinColumnsLineage(parentColumnsLineage, childColumnsLineage)
        p.children.map(extractColumnsLineage(_, nextColumnsLineage)).reduce(mergeColumnsLineage)

      case p: Generate =>
        val generateColumnsLineageWithId =
          ListMap(p.generatorOutput.map(attrRef => (attrRef.toAttribute.exprId, p.references)): _*)

        val nextColumnsLineage = parentColumnsLineage.map {
          case (key, attrRefs) =>
            key -> AttributeSet(attrRefs.flatMap(attr =>
              generateColumnsLineageWithId.getOrElse(
                attr.exprId,
                AttributeSet(attr))))
        }
        p.children.map(extractColumnsLineage(_, nextColumnsLineage)).reduce(mergeColumnsLineage)

      case p: Window =>
        val windowColumnsLineage =
          ListMap(p.windowExpressions.map(exp => (exp.toAttribute, exp.references)): _*)

        val nextColumnsLineage = if (parentColumnsLineage.isEmpty) {
          ListMap(p.child.output.map(attr => (attr, attr.references)): _*) ++ windowColumnsLineage
        } else {
          parentColumnsLineage.map {
            case (k, _) if windowColumnsLineage.contains(k) =>
              k -> windowColumnsLineage(k)
            case (k, attrs) =>
              k -> AttributeSet(attrs.flatten(attr =>
                windowColumnsLineage.getOrElse(attr, AttributeSet(attr))))
          }
        }
        p.children.map(extractColumnsLineage(_, nextColumnsLineage)).reduce(mergeColumnsLineage)

      case p: Join =>
        p.joinType match {
          case LeftSemi | LeftAnti =>
            extractColumnsLineage(p.left, parentColumnsLineage)
          case _ =>
            p.children.map(extractColumnsLineage(_, parentColumnsLineage))
              .reduce(mergeColumnsLineage)
        }

      case p: Union =>
        val childrenColumnsLineage =
          // support for the multi-insert statement
          if (p.output.isEmpty) {
            p.children
              .map(extractColumnsLineage(_, ListMap[Attribute, AttributeSet]()))
              .reduce(mergeColumnsLineage)
          } else {
            // merge all children in to one derivedColumns
            val childrenUnion =
              p.children.map(extractColumnsLineage(_, ListMap[Attribute, AttributeSet]())).map(
                _.values).reduce {
                (left, right) =>
                  left.zip(right).map(attr => attr._1 ++ attr._2)
              }
            ListMap(p.output.zip(childrenUnion): _*)
          }
        joinColumnsLineage(parentColumnsLineage, childrenColumnsLineage)

      case p: LogicalRelation if p.catalogTable.nonEmpty =>
        val tableName = getV1TableName(p.catalogTable.get.qualifiedName)
        joinRelationColumnLineage(parentColumnsLineage, p.output, Seq(tableName))

      case p: HiveTableRelation =>
        val tableName = getV1TableName(p.tableMeta.qualifiedName)
        joinRelationColumnLineage(parentColumnsLineage, p.output, Seq(tableName))

      case p: DataSourceV2ScanRelation =>
        val tableName = getV2TableName(p)
        joinRelationColumnLineage(parentColumnsLineage, p.output, Seq(tableName))

      // For creating the view from v2 table, the logical plan of table will
      // be the `DataSourceV2Relation` not the `DataSourceV2ScanRelation`.
      // because the view from the table is not going to read it.
      case p: DataSourceV2Relation =>
        val tableName = getV2TableName(p)
        joinRelationColumnLineage(parentColumnsLineage, p.output, Seq(tableName))

      case p: LocalRelation =>
        joinRelationColumnLineage(parentColumnsLineage, p.output, Seq(LOCAL_TABLE_IDENTIFIER))

      case _: OneRowRelation =>
        parentColumnsLineage.map {
          case (k, attrs) =>
            k -> AttributeSet(attrs.map {
              case attr
                  if attr.qualifier.nonEmpty && attr.qualifier.last.equalsIgnoreCase(
                    SUBQUERY_COLUMN_IDENTIFIER) =>
                attr.withQualifier(attr.qualifier.init)
              case attr => attr
            })
        }

      case p: View =>
        if (!p.isTempView && SparkContextHelper.getConf(
            LineageConf.SKIP_PARSING_PERMANENT_VIEW_ENABLED)) {
          val viewName = getV1TableName(p.desc.qualifiedName)
          joinRelationColumnLineage(parentColumnsLineage, p.output, Seq(viewName))
        } else {
          val viewColumnsLineage =
            extractColumnsLineage(p.child, ListMap[Attribute, AttributeSet]())
          mergeRelationColumnLineage(parentColumnsLineage, p.output, viewColumnsLineage)
        }

      case p: InMemoryRelation =>
        // get logical plan from cachedPlan
        val cachedTableLogical = findSparkPlanLogicalLink(Seq(p.cacheBuilder.cachedPlan))
        cachedTableLogical match {
          case Some(logicPlan) =>
            val relationColumnLineage =
              extractColumnsLineage(logicPlan, ListMap[Attribute, AttributeSet]())
            mergeRelationColumnLineage(parentColumnsLineage, p.output, relationColumnLineage)
          case _ =>
            joinRelationColumnLineage(
              parentColumnsLineage,
              p.output,
              p.cacheBuilder.tableName.toSeq)
        }

      case p if p.children.isEmpty => ListMap[Attribute, AttributeSet]()

      case p =>
        p.children.map(extractColumnsLineage(_, parentColumnsLineage)).reduce(mergeColumnsLineage)
    }
  }

  private def getQuery(plan: LogicalPlan): LogicalPlan = getField[LogicalPlan](plan, "query")

  private def getV2TableName(plan: NamedRelation): String = {
    plan match {
      case relation: DataSourceV2ScanRelation =>
        val catalog = relation.relation.catalog.map(_.name()).getOrElse(LineageConf.DEFAULT_CATALOG)
        val database = relation.relation.identifier.get.namespace().mkString(".")
        val table = relation.relation.identifier.get.name()
        s"$catalog.$database.$table"
      case relation: DataSourceV2Relation =>
        val catalog = relation.catalog.map(_.name()).getOrElse(LineageConf.DEFAULT_CATALOG)
        val database = relation.identifier.get.namespace().mkString(".")
        val table = relation.identifier.get.name()
        s"$catalog.$database.$table"
      case _ =>
        plan.name
    }
  }

  private def getV1TableName(qualifiedName: String): String = {
    qualifiedName.split("\\.") match {
      case Array(database, table) =>
        Seq(LineageConf.DEFAULT_CATALOG, database, table).filter(_.nonEmpty).mkString(".")
      case _ => qualifiedName
    }
  }
}

case class SparkSQLLineageParseHelper(sparkSession: SparkSession) extends LineageParser
  with Logging {

  def transformToLineage(
      executionId: Long,
      plan: LogicalPlan): Option[Lineage] = {
    Try(parse(plan)).recover {
      case e: Exception =>
        logWarning(s"Extract Statement[$executionId] columns lineage failed.", e)
        throw e
    }.toOption
  }

}
