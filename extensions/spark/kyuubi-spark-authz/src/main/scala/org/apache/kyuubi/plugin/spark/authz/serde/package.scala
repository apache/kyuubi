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

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.kyuubi.plugin.spark.authz.OperationType.{OperationType, QUERY}

package object serde {

  final val mapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build() :: ClassTagExtensions

  final lazy val DB_COMMAND_SPECS: Map[String, DatabaseCommandSpec] = {
    val is = getClass.getClassLoader.getResourceAsStream("database_command_spec.json")
    mapper.readValue[Array[DatabaseCommandSpec]](is).map(e => (e.classname, e)).toMap
  }

  final lazy val TABLE_COMMAND_SPECS: Map[String, TableCommandSpec] = {
    val is = getClass.getClassLoader.getResourceAsStream("table_command_spec.json")
    mapper.readValue[Array[TableCommandSpec]](is).map(e => (e.classname, e)).toMap
  }

  final lazy val FUNCTION_COMMAND_SPECS: Map[String, FunctionCommandSpec] = {
    val is = getClass.getClassLoader.getResourceAsStream("function_command_spec.json")
    mapper.readValue[Array[FunctionCommandSpec]](is).map(e => (e.classname, e)).toMap
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
