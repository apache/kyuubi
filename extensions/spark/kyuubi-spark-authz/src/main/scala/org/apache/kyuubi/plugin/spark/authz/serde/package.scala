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

import scala.reflect.ClassTag

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.kyuubi.plugin.spark.authz.OperationType.{OperationType, QUERY}
import org.apache.kyuubi.plugin.spark.authz.serde.ActionTypeExtractor.actionTypeExtractors
import org.apache.kyuubi.plugin.spark.authz.serde.CatalogExtractor.catalogExtractors
import org.apache.kyuubi.plugin.spark.authz.serde.ColumnExtractor.columnExtractors
import org.apache.kyuubi.plugin.spark.authz.serde.DatabaseExtractor.dbExtractors
import org.apache.kyuubi.plugin.spark.authz.serde.FunctionExtractor.functionExtractors
import org.apache.kyuubi.plugin.spark.authz.serde.FunctionTypeExtractor.functionTypeExtractors
import org.apache.kyuubi.plugin.spark.authz.serde.QueryExtractor.queryExtractors
import org.apache.kyuubi.plugin.spark.authz.serde.TableExtractor.tableExtractors
import org.apache.kyuubi.plugin.spark.authz.serde.TableTypeExtractor.tableTypeExtractors
import org.apache.kyuubi.util.reflect.ReflectUtils._

package object serde {

  final val mapper = JsonMapper.builder().addModule(DefaultScalaModule).build()

  def loadExtractorsToMap[T <: Extractor](implicit ct: ClassTag[T]): Map[String, T] =
    loadFromServiceLoader[T]()(ct).map { e: T => (e.key, e) }.toMap

  final lazy val DB_COMMAND_SPECS: Map[String, DatabaseCommandSpec] = {
    val is = getClass.getClassLoader.getResourceAsStream("database_command_spec.json")
    mapper.readValue(is, new TypeReference[Array[DatabaseCommandSpec]] {})
      .map(e => (e.classname, e)).toMap
  }

  final lazy val TABLE_COMMAND_SPECS: Map[String, TableCommandSpec] = {
    val is = getClass.getClassLoader.getResourceAsStream("table_command_spec.json")
    mapper.readValue(is, new TypeReference[Array[TableCommandSpec]] {})
      .map(e => (e.classname, e)).toMap
  }

  def isKnownTableCommand(r: AnyRef): Boolean = {
    TABLE_COMMAND_SPECS.contains(r.getClass.getName)
  }

  def getTableCommandSpec(r: AnyRef): TableCommandSpec = {
    TABLE_COMMAND_SPECS(r.getClass.getName)
  }

  final lazy val FUNCTION_COMMAND_SPECS: Map[String, FunctionCommandSpec] = {
    val is = getClass.getClassLoader.getResourceAsStream("function_command_spec.json")
    mapper.readValue(is, new TypeReference[Array[FunctionCommandSpec]] {})
      .map(e => (e.classname, e)).toMap
  }

  final private lazy val SCAN_SPECS: Map[String, ScanSpec] = {
    val is = getClass.getClassLoader.getResourceAsStream("scan_command_spec.json")
    mapper.readValue(is, new TypeReference[Array[ScanSpec]] {})
      .map(e => (e.classname, e))
      .filter(t => t._2.scanDescs.nonEmpty).toMap
  }

  def isKnownScan(r: AnyRef): Boolean = {
    SCAN_SPECS.contains(r.getClass.getName)
  }

  def getScanSpec(r: AnyRef): ScanSpec = {
    SCAN_SPECS(r.getClass.getName)
  }

  final private lazy val FUNCTION_SPECS: Map[String, ScanSpec] = {
    val is = getClass.getClassLoader.getResourceAsStream("scan_command_spec.json")
    mapper.readValue(is, new TypeReference[Array[ScanSpec]] {})
      .map(e => (e.classname, e))
      .filter(t => t._2.functionDescs.nonEmpty).toMap
  }

  def isKnownFunction(r: AnyRef): Boolean = {
    FUNCTION_SPECS.contains(r.getClass.getName)
  }

  def getFunctionSpec(r: AnyRef): ScanSpec = {
    FUNCTION_SPECS(r.getClass.getName)
  }

  def operationType(plan: LogicalPlan): OperationType = {
    val classname = plan.getClass.getName
    TABLE_COMMAND_SPECS.get(classname)
      .orElse(DB_COMMAND_SPECS.get(classname))
      .orElse(FUNCTION_COMMAND_SPECS.get(classname))
      .map(s => s.operationType)
      .getOrElse(QUERY)
  }

  /**
   * get extractor instance by extractor class name
   * @param extractorKey explicitly load extractor by its simple class name.
   *                           null by default means get extractor by extractor class.
   * @param ct class tag of extractor class type
   * @tparam T extractor class type
   * @return
   */
  def lookupExtractor[T <: Extractor](extractorKey: String)(
      implicit ct: ClassTag[T]): T = {
    val extractorClass = ct.runtimeClass
    val extractors: Map[String, Extractor] = extractorClass match {
      case c if classOf[CatalogExtractor].isAssignableFrom(c) => catalogExtractors
      case c if classOf[DatabaseExtractor].isAssignableFrom(c) => dbExtractors
      case c if classOf[TableExtractor].isAssignableFrom(c) => tableExtractors
      case c if classOf[TableTypeExtractor].isAssignableFrom(c) => tableTypeExtractors
      case c if classOf[ColumnExtractor].isAssignableFrom(c) => columnExtractors
      case c if classOf[QueryExtractor].isAssignableFrom(c) => queryExtractors
      case c if classOf[FunctionExtractor].isAssignableFrom(c) => functionExtractors
      case c if classOf[FunctionTypeExtractor].isAssignableFrom(c) => functionTypeExtractors
      case c if classOf[ActionTypeExtractor].isAssignableFrom(c) => actionTypeExtractors
      case _ => throw new IllegalArgumentException(s"Unknown extractor type: $ct")
    }
    extractors(extractorKey).asInstanceOf[T]
  }

  def lookupExtractor[T <: Extractor](implicit ct: ClassTag[T]): T =
    lookupExtractor[T](ct.runtimeClass.getSimpleName)(ct)
}
