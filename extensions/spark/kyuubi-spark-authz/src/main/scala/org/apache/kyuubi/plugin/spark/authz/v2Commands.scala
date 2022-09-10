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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.Identifier

import org.apache.kyuubi.plugin.spark.authz.OperationType.{CREATEDATABASE, CREATETABLE, OperationType, QUERY}
import org.apache.kyuubi.plugin.spark.authz.PrivilegesBuilder._
import org.apache.kyuubi.plugin.spark.authz.V2CommandType.{HasQuery, V2CommandType, V2CreateTablePlan}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.{getFieldVal, invoke, isSparkVersionAtLeast, isSparkVersionAtMost, quote}

object V2CommandType extends Enumeration {
  type V2CommandType = Value

  // traits' name from Spark v2commands
  val V2CreateTablePlan, V2WriteCommand = Value

  // with query plan
  val HasQuery = Value
}

object v2Commands extends Enumeration {

  import scala.language.implicitConversions

  implicit def valueToV2CommandBuilder(x: Value): V2Command =
    x.asInstanceOf[V2Command]

  def accept(commandName: String): Boolean = {
    try {
      val cmd = v2Commands.withName(commandName)

      // check spark version requirements
      (StringUtils.isBlank(cmd.mostVer) || isSparkVersionAtMost(cmd.mostVer)) &&
      (StringUtils.isBlank(cmd.leastVer) || isSparkVersionAtLeast(cmd.leastVer))
    } catch {
      case _: NoSuchElementException => false
    }
  }

  val defaultBuildInput: (LogicalPlan, ArrayBuffer[PrivilegeObject], Seq[V2CommandType]) => Unit =
    (p, inputObjs, cmdTypes) => {
      cmdTypes.foreach {
        case HasQuery =>
          val query = getFieldVal[LogicalPlan](p, "query")
          buildQuery(query, inputObjs)
        case _ =>
      }
    }

  val defaultBuildOutput: (LogicalPlan, ArrayBuffer[PrivilegeObject], Seq[V2CommandType]) => Unit =
    (p, outputObjs, cmdTypes) => {
      cmdTypes.foreach {
        case V2CreateTablePlan =>
          val table = invoke(p, "tableName").asInstanceOf[Identifier]
          outputObjs += v2TablePrivileges(table)
        case _ =>
      }
    }

  case class V2Command(
      leastVer: String = "",
      mostVer: String = "",
      buildInput: (LogicalPlan, ArrayBuffer[PrivilegeObject], Seq[V2CommandType]) => Unit =
        defaultBuildInput,
      buildOutput: (LogicalPlan, ArrayBuffer[PrivilegeObject], Seq[V2CommandType]) => Unit =
        defaultBuildOutput,
      operType: OperationType = QUERY,
      cmdTypes: Seq[V2CommandType] = Seq())
    extends super.Val {

    def handle(
        plan: LogicalPlan,
        inputObjs: ArrayBuffer[PrivilegeObject],
        outputObjs: ArrayBuffer[PrivilegeObject]): Unit = {
      this.buildInput(plan, inputObjs, cmdTypes)
      this.buildOutput(plan, outputObjs, cmdTypes)
    }
  }

  // commands

  val CreateNamespace: V2Command = V2Command(
    operType = CREATEDATABASE,
    buildOutput = (plan, outputObjs, _) => {
      if (isSparkVersionAtLeast("3.3")) {
        val resolvedNamespace = getFieldVal[Any](plan, "name")
        val databases = getFieldVal[Seq[String]](resolvedNamespace, "nameParts")
        outputObjs += databasePrivileges(quote(databases))
      } else {
        val namespace = getFieldVal[Seq[String]](plan, "namespace")
        outputObjs += databasePrivileges(quote(namespace))
      }
    })

  // with V2CreateTablePlan

  val CreateTable: V2Command = V2Command(
    operType = CREATETABLE,
    cmdTypes = Seq(V2CreateTablePlan),
    leastVer = "3.3")

  val CreateV2Table: V2Command = V2Command(
    operType = CREATETABLE,
    cmdTypes = Seq(V2CreateTablePlan),
    mostVer = "3.2")

  val CreateTableAsSelect: V2Command = V2Command(
    operType = CREATETABLE,
    cmdTypes = Seq(V2CreateTablePlan, HasQuery))

  val ReplaceTable: V2Command = V2Command(
    operType = CREATETABLE,
    cmdTypes = Seq(V2CreateTablePlan))

  val ReplaceTableAsSelect: V2Command = V2Command(
    operType = CREATETABLE,
    cmdTypes = Seq(V2CreateTablePlan, HasQuery))

}
