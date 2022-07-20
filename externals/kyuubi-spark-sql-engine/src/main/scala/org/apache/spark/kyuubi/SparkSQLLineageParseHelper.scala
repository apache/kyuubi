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

package org.apache.spark.kyuubi

import scala.collection.immutable.ListMap
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil.isSparkVersionAtMost
import org.apache.kyuubi.engine.spark.events.Lineage

trait LineageParser {
  def sparkSession: SparkSession

  def parse(plan: LogicalPlan): Lineage = {
    val ret = {
      extractColumnsLineage(plan, ListMap()).toList.collect {
        case (k, columns) =>
          k.split('#').head -> columns.map(_.split('#').head)
      }
    }
    val (inputTables, outputTables) = ret.foldLeft((List[String](), List[String]())) {
      case ((inputs, outputs), (out, in)) =>
        val x = (inputs ++ in.map(_.split('.').init.mkString("."))).filter(_.nonEmpty)
        val y = outputs ++ List(out.split('.').init.mkString(".")).filter(_.nonEmpty)
        (x, y)
    }
    Lineage(inputTables.distinct, outputTables.distinct, ret)
  }

  private def mergeColumnsLineage(
      left: ListMap[String, List[String]],
      right: ListMap[String, List[String]]): ListMap[String, List[String]] = {
    if (left.isEmpty || right.isEmpty) left ++ right
    else {
      val head =
        ListMap(left.head._1 -> (left.head._2 ++ right.getOrElse(left.head._1, List.empty)))
      head ++ mergeColumnsLineage(left.tail, right - left.head._1)
    }
  }

  private def joinColumnsLineage(
      parent: ListMap[String, List[String]],
      child: ListMap[String, List[String]]): ListMap[String, List[String]] = {
    if (parent.isEmpty) child
    else {
      val childTmp = child.map { case (k, v) => (k.split('#').last, v) }
      parent.map { case (k, columns) =>
        k -> {
          columns.flatMap {
            case col if childTmp.contains(col.split('#').last) => childTmp(col.split('#').last)
            case _ => List()
          }
        }
      }
    }
  }

  private def extractColumnsLineage(
      plan: LogicalPlan,
      parentColumnsLineage: ListMap[String, List[String]]): ListMap[String, List[String]] = {

    def getPlanField[T](field: String): T = {
      getFieldVal[T](plan, field)
    }

    def getCurrentPlanField[T](curPlan: LogicalPlan, field: String): T = {
      getFieldVal[T](curPlan, field)
    }

    def getPlanMethod[T](name: String): T = {
      getMethod[T](plan, name)
    }

    def getQuery: LogicalPlan = {
      getPlanField[LogicalPlan]("query")
    }

    plan match {
      // For command
      case p if p.nodeName == "AlterViewAsCommand" =>
        val analyzed = sparkSession.sessionState.analyzer.execute(getQuery)
        val view = getPlanField[TableIdentifier]("name").unquotedString
        extractColumnsLineage(analyzed, parentColumnsLineage).map { case (k, v) =>
          (s"$view.$k", v)
        }

      case p if p.nodeName == "CreateViewCommand" =>
        val view = getPlanField[TableIdentifier]("name").unquotedString
        val outputCols =
          getPlanField[Seq[(String, Option[String])]]("userSpecifiedColumns").map(_._1)
        val query =
          if (isSparkVersionAtMost("3.1")) {
            getPlanField[LogicalPlan]("child")
          } else {
            getPlanField[LogicalPlan]("plan")
          }
        val analyzed = sparkSession.sessionState.analyzer.execute(query)
        extractColumnsLineage(analyzed, parentColumnsLineage).zipWithIndex.map {
          case (x, i) if outputCols.nonEmpty => s"$view.${outputCols(i)}" -> x._2
          case ((k, v), _) => s"$view.$k" -> v
        }

      case p if p.nodeName == "CreateDataSourceTableAsSelectCommand" =>
        val table = getPlanField[CatalogTable]("table").qualifiedName
        val analyzed = sparkSession.sessionState.analyzer.execute(getQuery)
        extractColumnsLineage(analyzed, parentColumnsLineage).map { case (k, v) =>
          s"$table.$k" -> v
        }

      case p
          if p.nodeName == "CreateHiveTableAsSelectCommand" ||
            p.nodeName == "OptimizedCreateHiveTableAsSelectCommand" =>
        val table = getPlanField[CatalogTable]("tableDesc").qualifiedName
        extractColumnsLineage(getQuery, parentColumnsLineage).map { case (k, v) =>
          s"$table.$k" -> v
        }

      case p
          if p.nodeName == "CreateTableAsSelect" ||
            p.nodeName == "ReplaceTableAsSelect" =>
        val (table, namespace, catalog) =
          if (isSparkVersionAtMost("3.2")) {
            (
              getPlanField[Identifier]("tableName").name,
              getPlanField[Identifier]("tableName").namespace.mkString("."),
              getPlanField[TableCatalog]("catalog").name())
          } else {
            (
              getPlanMethod[Identifier]("tableName").name(),
              getPlanMethod[Identifier]("tableName").namespace().mkString("."),
              getCurrentPlanField[CatalogPlugin](
                getPlanMethod[LogicalPlan]("left"),
                "catalog").name())
          }
        extractColumnsLineage(getQuery, parentColumnsLineage).map { case (k, v) =>
          s"$catalog.$namespace.$table.$k" -> v
        }

      case p if p.nodeName == "InsertIntoDataSourceCommand" =>
        val logicalRelation = getPlanField[LogicalRelation]("logicalRelation")
        val table = logicalRelation.catalogTable.map(_.qualifiedName).getOrElse("")
        extractColumnsLineage(getQuery, parentColumnsLineage).map { case (k, v) =>
          s"$table.$k" -> v
        }

      case p if p.nodeName == "InsertIntoHadoopFsRelationCommand" =>
        val table =
          getPlanField[Option[CatalogTable]]("catalogTable").map(_.qualifiedName).getOrElse("")
        extractColumnsLineage(getQuery, parentColumnsLineage).map { case (k, v) =>
          s"$table.$k" -> v
        }

      case p
          if p.nodeName == "InsertIntoDataSourceDirCommand" ||
            p.nodeName == "InsertIntoHiveDirCommand" =>
        val dir =
          getPlanField[CatalogStorageFormat]("storage").locationUri.map(_.toString).getOrElse("")
        extractColumnsLineage(getQuery, parentColumnsLineage).map { case (k, v) =>
          s"`$dir`.$k" -> v
        }

      case p if p.nodeName == "InsertIntoHiveTable" =>
        val table = getPlanField[CatalogTable]("table").qualifiedName
        extractColumnsLineage(getQuery, parentColumnsLineage).map { case (k, v) =>
          s"$table.$k" -> v
        }

      case p if p.nodeName == "SaveIntoDataSourceCommand" =>
        // TODO: Should get the table via datasource options?
        extractColumnsLineage(getQuery, parentColumnsLineage)

      // For query
      case p: Project =>
        val nextColumnsLineage =
          joinColumnsLineage(parentColumnsLineage, getSelectColumnLineage(p.projectList))
        p.children.map(extractColumnsLineage(_, nextColumnsLineage)).reduce(mergeColumnsLineage)

      case p: Aggregate =>
        val nextColumnsLineage =
          joinColumnsLineage(parentColumnsLineage, getSelectColumnLineage(p.aggregateExpressions))
        p.children.map(extractColumnsLineage(_, nextColumnsLineage)).reduce(mergeColumnsLineage)

      case p: Union =>
        // merge all children in to one derivedColumns
        val childrenUnion =
          p.children.map(extractColumnsLineage(_, ListMap())).map(_.values).reduce {
            (left, right) =>
              left.zip(right).map(col => (col._1 ++ col._2).distinct)
          }
        val childrenColumnsLineage =
          ListMap(p.output.map(a => s"${a.name}#${a.exprId.id}").toList.zip(childrenUnion): _*)
        joinColumnsLineage(parentColumnsLineage, childrenColumnsLineage)

      case p: SubqueryAlias =>
        extractColumnsLineage(p.child, parentColumnsLineage)

      case p: LogicalRelation if p.catalogTable.nonEmpty =>
        val tableName = p.catalogTable.get.qualifiedName
        val dataColIds = p.output.map(_.exprId.id.toString).toSet
        val dataColNames = p.output.map(_.name)
        if (parentColumnsLineage.nonEmpty) {
          parentColumnsLineage.map {
            case (k, columns) =>
              k -> columns.collect {
                case col if dataColIds.contains(col.split('#').last) => s"$tableName.$col"
              }
          }
        } else {
          val colPair = dataColNames.map { col =>
            col -> List(s"$tableName.$col")
          }
          ListMap(colPair: _*)
        }

      case p: HiveTableRelation =>
        val tableName = p.tableMeta.qualifiedName
        val dataCols = p.dataCols.map(_.exprId.id.toString).toSet
        val dataColNames = p.output.map(_.name)
        if (parentColumnsLineage.nonEmpty) {
          parentColumnsLineage.map { case (k, columns) =>
            k -> columns.collect {
              case col if dataCols.contains(col.split('#').last) => s"$tableName.$col"
            }
          }
        } else {
          val colPair = dataColNames.map { col =>
            col -> List(s"$tableName.$col")
          }
          ListMap(colPair: _*)
        }

      case p: DataSourceV2Relation =>
        val tableName = p.name
        val dataCols = p.output.map(_.exprId.id.toString).toSet
        val dataColNames = p.output.map(_.name)
        if (parentColumnsLineage.nonEmpty) {
          parentColumnsLineage.map { case (k, columns) =>
            k -> columns.collect {
              case col if dataCols.contains(col.split('#').last) => s"$tableName.$col"
            }
          }
        } else {
          val colPair = dataColNames.map { col =>
            col -> List(s"$tableName.$col")
          }
          ListMap(colPair: _*)
        }

      case p: DataSourceV2ScanRelation =>
        val tableName = p.name
        val dataCols = p.output.map(_.exprId.id.toString).toSet
        val dataColNames = p.output.map(_.name)
        if (parentColumnsLineage.nonEmpty) {
          parentColumnsLineage.map { case (k, columns) =>
            k -> columns.collect {
              case col if dataCols.contains(col.split('#').last) => s"$tableName.$col"
            }
          }
        } else {
          val colPair = dataColNames.map { col =>
            col -> List(s"$tableName.$col")
          }
          ListMap(colPair: _*)
        }

      case p: LocalRelation =>
        val dataCols = p.output.map(_.exprId.id.toString).toSet
        val dataColNames = p.output.map(_.name)
        if (parentColumnsLineage.nonEmpty) {
          parentColumnsLineage.map { case (k, columns) =>
            k -> columns.collect {
              case col if dataCols.contains(col.split('#').last) => col
            }
          }
        } else {
          val colPair = dataColNames.map { col =>
            col -> List(col)
          }
          ListMap(colPair: _*)
        }

      case p if p.children.isEmpty => ListMap.empty
      case p =>
        p.children.map(extractColumnsLineage(_, parentColumnsLineage)).reduce(mergeColumnsLineage)
    }
  }

  private def getSelectColumnLineage(exps: Seq[NamedExpression]): ListMap[String, List[String]] = {
    val result = exps.map { exp =>
      val derivedColumns = exp.references.map(e => s"${e.name}#${e.exprId.id}").toList
      exp match {
        case e: Alias =>
          (s"${e.name}#${e.exprId.id}" -> derivedColumns)
        case _ =>
          (exp.toString -> derivedColumns)
      }
    }.toList
    ListMap(result: _*)
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
      statementId: String,
      plan: LogicalPlan): Option[Lineage] = {
    Try(parse(plan)).recover {
      case e: Exception =>
        warn(s"Extract Statement[$statementId] columns lineage failed.", e)
        throw e
    }.toOption
  }

}
