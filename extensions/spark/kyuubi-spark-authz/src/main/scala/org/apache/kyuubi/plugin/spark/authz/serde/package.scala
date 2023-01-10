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

import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.kyuubi.plugin.spark.authz.OperationType.{OperationType, QUERY}

package object serde {

  final val mapper = JsonMapper.builder().addModule(DefaultScalaModule).build()

  def loadExtractorsToMap[T <: Extractor](implicit ct: ClassTag[T]): Map[String, T] = {
    ServiceLoader.load(ct.runtimeClass).iterator().asScala
      .map { case e: Extractor => (e.key, e.asInstanceOf[T]) }
      .toMap
  }

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
      .map(e => (e.classname, e)).toMap
  }

  def isKnownScan(r: AnyRef): Boolean = {
    SCAN_SPECS.contains(r.getClass.getName)
  }

  def getScanSpec(r: AnyRef): ScanSpec = {
    SCAN_SPECS(r.getClass.getName)
  }

  def operationType(plan: LogicalPlan): OperationType = {
    val classname = plan.getClass.getName
    TABLE_COMMAND_SPECS.get(classname)
      .orElse(DB_COMMAND_SPECS.get(classname))
      .orElse(FUNCTION_COMMAND_SPECS.get(classname))
      .map(s => s.operationType)
      .getOrElse(QUERY)
  }
}
