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
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.ScalarSubquery
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

import org.apache.kyuubi.Logging
import org.apache.kyuubi.plugin.lineage.events.Lineage
import org.apache.kyuubi.plugin.lineage.helper.SparkListenerHelper.isSparkVersionAtMost

trait LineageParser {
  def sparkSession: SparkSession

  type AttributeMap[A] = ListMap[Attribute, A]

  def parse(plan: LogicalPlan): Lineage = {
    val columnsLineage =
      extractColumnsLineage(plan, ListMap[Attribute, AttributeSet]()).toList.collect {
        case (k, attrs) =>
          k.name -> attrs.map(_.name).toSet
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
          childMap.getOrElse(attr.exprId, AttributeSet.empty)))
      }
    }
  }

  private def getExpressionSubqueryPlans(expression: Expression): Seq[LogicalPlan] = {
    expression match {
      case s: ScalarSubquery => Seq(s.plan)
      case s => s.children.flatMap(getExpressionSubqueryPlans)
    }
  }

  private def getSelectColumnLineage(
      named: Seq[NamedExpression]): AttributeMap[AttributeSet] = {
    val exps = named.map {
      case exp: Alias =>
        if (exp.references.nonEmpty) exp.toAttribute -> exp.references
        else {
          exp.toAttribute ->
            getExpressionSubqueryPlans(exp.child)
              .map(extractColumnsLineage(_, ListMap[Attribute, AttributeSet]()))
              .foldLeft(ListMap[Attribute, AttributeSet]())(mergeColumnsLineage).values
              .foldLeft(AttributeSet.empty)(_ ++ _)
        }
      case a: Attribute => a -> a.references
    }
    ListMap(exps: _*)
  }

  private def joinRelationColumnLineage(
      parent: AttributeMap[AttributeSet],
      relationAttrs: Seq[Attribute],
      tableName: String = ""): AttributeMap[AttributeSet] = {
    val relationAttrSet = AttributeSet(relationAttrs)
    if (parent.nonEmpty) {
      parent.map { case (k, attrs) =>
        k -> AttributeSet(attrs.collect {
          case attr if relationAttrSet.contains(attr) =>
            attr.withName(Seq(tableName, attr.name).filter(_.nonEmpty).mkString("."))
          case attr if attr.name.contains(".") =>
            attr
        })
      }
    } else {
      ListMap(relationAttrs.map { attr =>
        (
          attr,
          AttributeSet(attr.withName(Seq(tableName, attr.name).filter(_.nonEmpty).mkString("."))))
      }: _*)
    }
  }

  private def extractColumnsLineage(
      plan: LogicalPlan,
      parentColumnsLineage: AttributeMap[AttributeSet]): AttributeMap[AttributeSet] = {

    plan match {
      // For command
      case p if p.nodeName == "AlterViewAsCommand" =>
        val query =
          if (isSparkVersionAtMost("3.1")) {
            sparkSession.sessionState.analyzer.execute(getQuery(plan))
          } else {
            getQuery(plan)
          }
        val view = getPlanField[TableIdentifier]("name", plan).unquotedString
        extractColumnsLineage(query, parentColumnsLineage).map { case (k, v) =>
          k.withName(s"$view.${k.name}") -> v
        }

      case p if p.nodeName == "CreateViewCommand" =>
        val view = getPlanField[TableIdentifier]("name", plan).unquotedString
        val outputCols =
          getPlanField[Seq[(String, Option[String])]]("userSpecifiedColumns", plan).map(_._1)
        val query =
          if (isSparkVersionAtMost("3.1")) {
            sparkSession.sessionState.analyzer.execute(getPlanField[LogicalPlan]("child", plan))
          } else {
            getPlanField[LogicalPlan]("plan", plan)
          }
        extractColumnsLineage(query, parentColumnsLineage).zipWithIndex.map {
          case ((k, v), i) if outputCols.nonEmpty => k.withName(s"$view.${outputCols(i)}") -> v
          case ((k, v), _) => k.withName(s"$view.${k.name}") -> v
        }

      case p if p.nodeName == "CreateDataSourceTableAsSelectCommand" =>
        val table = getPlanField[CatalogTable]("table", plan).qualifiedName
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map { case (k, v) =>
          k.withName(s"$table.${k.name}") -> v
        }

      case p
          if p.nodeName == "CreateHiveTableAsSelectCommand" ||
            p.nodeName == "OptimizedCreateHiveTableAsSelectCommand" =>
        val table = getPlanField[CatalogTable]("tableDesc", plan).qualifiedName
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map { case (k, v) =>
          k.withName(s"$table.${k.name}") -> v
        }

      case p
          if p.nodeName == "CreateTableAsSelect" ||
            p.nodeName == "ReplaceTableAsSelect" =>
        val (table, namespace, catalog) =
          if (isSparkVersionAtMost("3.2")) {
            (
              getPlanField[Identifier]("tableName", plan).name,
              getPlanField[Identifier]("tableName", plan).namespace.mkString("."),
              getPlanField[TableCatalog]("catalog", plan).name())
          } else {
            (
              getPlanMethod[Identifier]("tableName", plan).name(),
              getPlanMethod[Identifier]("tableName", plan).namespace().mkString("."),
              getCurrentPlanField[CatalogPlugin](
                getPlanMethod[LogicalPlan]("left", plan),
                "catalog").name())
          }
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map { case (k, v) =>
          k.withName(Seq(catalog, namespace, table, k.name).filter(_.nonEmpty).mkString(".")) -> v
        }

      case p if p.nodeName == "InsertIntoDataSourceCommand" =>
        val logicalRelation = getPlanField[LogicalRelation]("logicalRelation", plan)
        val table = logicalRelation.catalogTable.map(_.qualifiedName).getOrElse("")
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map {
          case (k, v) if table.nonEmpty =>
            k.withName(s"$table.${k.name}") -> v
        }

      case p if p.nodeName == "InsertIntoHadoopFsRelationCommand" =>
        val table =
          getPlanField[Option[CatalogTable]]("catalogTable", plan).map(_.qualifiedName).getOrElse(
            "")
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map {
          case (k, v) if table.nonEmpty =>
            k.withName(s"$table.${k.name}") -> v
        }

      case p
          if p.nodeName == "InsertIntoDataSourceDirCommand" ||
            p.nodeName == "InsertIntoHiveDirCommand" =>
        val dir =
          getPlanField[CatalogStorageFormat]("storage", plan).locationUri.map(_.toString).getOrElse(
            "")
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map {
          case (k, v) if dir.nonEmpty =>
            k.withName(s"`$dir`.${k.name}") -> v
        }

      case p if p.nodeName == "InsertIntoHiveTable" =>
        val table = getPlanField[CatalogTable]("table", plan).qualifiedName
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map { case (k, v) =>
          k.withName(s"$table.${k.name}") -> v
        }

      case p if p.nodeName == "SaveIntoDataSourceCommand" =>
        extractColumnsLineage(getQuery(plan), parentColumnsLineage)

      // For query
      case p: Project =>
        val nextColumnsLineage =
          joinColumnsLineage(parentColumnsLineage, getSelectColumnLineage(p.projectList))
        p.children.map(extractColumnsLineage(_, nextColumnsLineage)).reduce(mergeColumnsLineage)

      case p: Aggregate =>
        val nextColumnsLineage =
          joinColumnsLineage(parentColumnsLineage, getSelectColumnLineage(p.aggregateExpressions))
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
        // merge all children in to one derivedColumns
        val childrenUnion =
          p.children.map(extractColumnsLineage(_, ListMap[Attribute, AttributeSet]())).map(
            _.values).reduce {
            (left, right) =>
              left.zip(right).map(attr => attr._1 ++ attr._2)
          }
        val childrenColumnsLineage = ListMap(p.output.zip(childrenUnion): _*)
        joinColumnsLineage(parentColumnsLineage, childrenColumnsLineage)

      case p: LogicalRelation if p.catalogTable.nonEmpty =>
        val tableName = p.catalogTable.get.qualifiedName
        joinRelationColumnLineage(parentColumnsLineage, p.output, tableName)

      case p: HiveTableRelation =>
        val tableName = p.tableMeta.qualifiedName
        joinRelationColumnLineage(parentColumnsLineage, p.output, tableName)

      case p: DataSourceV2ScanRelation =>
        val tableName = p.name
        joinRelationColumnLineage(parentColumnsLineage, p.output, tableName)

      case p: LocalRelation =>
        joinRelationColumnLineage(parentColumnsLineage, p.output, "__local__")

      case p if p.children.isEmpty => ListMap[Attribute, AttributeSet]()

      case p =>
        p.children.map(extractColumnsLineage(_, parentColumnsLineage)).reduce(mergeColumnsLineage)
    }
  }

  private def getPlanField[T](field: String, plan: LogicalPlan): T = {
    getFieldVal[T](plan, field)
  }

  private def getCurrentPlanField[T](curPlan: LogicalPlan, field: String): T = {
    getFieldVal[T](curPlan, field)
  }

  private def getPlanMethod[T](name: String, plan: LogicalPlan): T = {
    getMethod[T](plan, name)
  }

  private def getQuery(plan: LogicalPlan): LogicalPlan = {
    getPlanField[LogicalPlan]("query", plan)
  }

  private def getFieldVal[T](o: Any, name: String): T = {
    Try {
      val field = o.getClass.getDeclaredField(name)
      field.setAccessible(true)
      field.get(o)
    } match {
      case Success(value) => value.asInstanceOf[T]
      case Failure(e) =>
        val candidates = o.getClass.getDeclaredFields.map(_.getName).mkString("[", ",", "]")
        throw new RuntimeException(s"$name not in $candidates", e)
    }
  }

  private def getMethod[T](o: Any, name: String): T = {
    Try {
      val method = o.getClass.getDeclaredMethod(name)
      method.invoke(o)
    } match {
      case Success(value) => value.asInstanceOf[T]
      case Failure(e) =>
        val candidates = o.getClass.getDeclaredMethods.map(_.getName).mkString("[", ",", "]")
        throw new RuntimeException(s"$name not in $candidates", e)
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
        warn(s"Extract Statement[$executionId] columns lineage failed.", e)
        throw e
    }.toOption
  }

}
